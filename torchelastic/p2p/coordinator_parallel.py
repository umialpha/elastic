#!/usr/bin/env python3

# Copyright (c) Facebook, Inc. and its affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.


import json
import logging
import os
import socket
import traceback
from datetime import timedelta
import threading
import time

import torch
import torch.distributed as dist

# Register handler for etcd-based rendezous:
import torchelastic.rendezvous.etcd_rendezvous  # noqa: F401
from torchelastic import metrics
from torchelastic.coordinator import Coordinator, NonRetryableException, StopException
from torchelastic.event_logger import get_event_logger
from torchelastic.rendezvous import RendezvousClosedException, RendezvousHandler


# Logger
log = logging.getLogger("CoordinatorParallel")
log.setLevel(logging.INFO)


def cas_delay():
    time.sleep(random.uniform(0, 0.1))


class CoordinatorParallel(Coordinator):
    # monitoring progress is expensive as it requires a collective communication
    # operation. MONITOR_PROGRESS_FREQ controls how often we run this.
    MONITOR_PROGRESS_FREQ = 1000

    def __init__(
        self,
        c10d_backend,
        init_method,
        max_num_trainers,
        process_group_timeout=10000,
        coordinator_pg_timeout=600000,  # default 10 mins for coordinator pg timeout
    ):
        self.c10d_backend = c10d_backend
        self.init_method = init_method
        self.rendezvous = dist.rendezvous(init_method)
        assert isinstance(
            self.rendezvous, RendezvousHandler
        ), "CoordinatorParallel requires a torchelastic.rendezvous.RendezvousHandler"
        assert coordinator_pg_timeout > process_group_timeout, (
            "coordinator_pg_timeout {} (ms) must larger than or equal to "
            "process_group_timeout {} (ms)".format(
                coordinator_pg_timeout, process_group_timeout
            )
        )
        self.max_num_trainers = max_num_trainers
        self.process_group_timeout = process_group_timeout
        self.coordinator_pg_timeout = coordinator_pg_timeout
        self.rank = -1
        self.world_size = 0
        self.is_worker_straggler = False
        self.stop_training = False
        self.coordinator_process_group = None
        self.monitor_progress_step = 0
        self.host_name = socket.gethostname()
        self.pid = os.getpid()
        self.event_logger = get_event_logger()
        metrics.initialize_metrics()
        self._stopped_event = threading.Event()
        self.lock = threading.Lock()
        self.is_initialized = False
        threading.Thread(
            target=self.run
        )

    def _log_event(self, event_name, message=None):
        if message is None:
            message = {}
        message["event_name"] = event_name
        message["host_name"] = self.host_name
        message["pid"] = self.pid
        message["rank"] = self.rank
        self.event_logger.log_event(event_name, json.dumps(message))

    def _destroy_process_group(self):
        if dist.is_initialized():
            if self.c10d_backend != dist.Backend.GLOO:
                dist.destroy_process_group(self.coordinator_process_group)
            dist.destroy_process_group()


    def run(self):
        while True:
            if self._stopped_event.is_set():
                break
            if not self.is_initialized or self._should_rendezvous():
                store, rank, world_size = self._rendezvous_barrier()
                with self.lock:
                    self._rendezvous_flag = True
                    self.next_store, self.next_rank, self.next_world_size = store, rank, world_size
                    self.is_initialized = True
            
        

    @metrics.profile("torchelastic")
    def _rendezvous_barrier(self):
        try:
            self._log_event("rendezvous_started")
            store, rank, world_size = self.rendezvous.next_rendezvous()
            self._log_event("rendezvous_succeeded", {"word_size": self.world_size})
        except RendezvousClosedException:
            # Sets the local variable to True
            self._log_event("rendezvous_closed")
            raise StopException(
                "Rank {0} received RendezvousClosedException."
                " Raising a StopException".format(self.rank)
            )
        except Exception as e:
            self._log_event("rendezvous_failed")
            raise NonRetryableException(
                "Rank {0} received an Exception."
                " Detailed message: {1}".format(self.rank, str(e))
            )
        log.info(
            "Got next rendezvous: rank {0}, world size {1}".format(
                self.rank, self.world_size
            )
        )

        # Assume straggler state is unreliable after rendezvous
        self.is_worker_straggler = False
        return store, rank, world_size

    def rendezvous_barrier(self):
        while not self.is_initialized():
            cas_delay()
        with self.lock:
            self.store, self.rank, self.world_size = self.next_store, self.next_rank, self.next_world_size
            self._rendezvous_flag = False
            self._destroy_process_group()
            self.should_stop_training = True
        return self.store, self.rank, self.world_size
    

    def barrier(self):
        # Use gloo process group to implement a barrier in case NCCL get stuck
        # Note there is an implicit timeout for barrier, which equal coordinator_pg_timeout
        dist.barrier(group=self.coordinator_process_group)

    @metrics.profile("torchelastic")
    def init_process_group(self):
        self.monitor_progress_step = 0
        dist.init_process_group(
            self.c10d_backend,
            timeout=timedelta(milliseconds=self.process_group_timeout),
            world_size=self.world_size,
            rank=self.rank,
            store=self.store,
        )

        if self.c10d_backend == dist.Backend.GLOO:
            self.coordinator_process_group = dist.group.WORLD
        else:
            # We don't need to use NCCL process group for control plane
            # collective operations, this helps us simplify our code (no need
            # to make it portable with NCCL)
            self.coordinator_process_group = dist.new_group(
                backend=dist.distributed_c10d.Backend.GLOO,
                timeout=timedelta(milliseconds=self.coordinator_pg_timeout),
            )

        log.info(
            "Initialized process group rank {0}, world size {1}".format(
                self.rank, self.world_size
            )
        )

    @metrics.profile("torchelastic")
    def should_save_checkpoint(self):
        """
        Whether the PET training loop need to do checkpoint.
        This normally happens when the job was explicitly ask for checkpoint.
        eg: executor got a preemption from scheduler
        """
        return False

    @metrics.profile("torchelastic")
    def _should_rendezvous(self):
        if dist.get_world_size() == self.max_num_trainers:
            return False

        # Check if there are any new workers waiting at the rendezvous barrier
        num_new_nodes = torch.LongTensor([self.rendezvous.num_nodes_waiting()])

        # Use the GLOO based coordinator_process_group to perform the
        # collective op as we don't want to transfer these back-and-forth
        # between GPU and CPU (when GPUs are available).
        dist.all_reduce(
            num_new_nodes, op=dist.ReduceOp.MAX, group=self.coordinator_process_group
        )

        if num_new_nodes > 0:
            log.info(
                "Rank {0} detected {1} new nodes; will re-rendezvous.".format(
                    self.rank, num_new_nodes[0]
                )
            )
            return True
        else:
            return False

    def should_rendezvous(self):
        return self._rendezvous_flag



    @metrics.profile("torchelastic")
    def should_stop_training(self):
        # Check if coordinator wants the training to stop
        return self.stop_training

    @metrics.profile("torchelastic")
    def signal_training_done(self):
        # Close the rendezvous to indicate termination of the overall execution.
        # This also propagates the stop signal to other trainers.
        self.rendezvous.set_closed()
        self._destroy_process_group()
        self.stop_training = True

    