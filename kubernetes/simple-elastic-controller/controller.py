import random
import time
import argparse
import os

from kubernetes import client, config
import yaml


INCREASE = 0
STAY = 1
DECREASE = 2

parser = argparse.ArgumentParser()
parser.add_argument(
    "-job_name",
    required=True,
    help="define the job name",
)
parser.add_argument(
    "-namespace",
    required=True,
    help="define the namespace, should be the same of etcd pod",
)
parser.add_argument(
    "-min_worker",
    required=True,
    type=int,
    help="define the number of minimum workers, >= 1",
)

parser.add_argument(
    "-max_worker",
    required=True,
    type=int,
    help="define the number of maximum workers",
)

parser.add_argument(
    "--r",
    default=False,
    help="If set True, it will randomly increase or decrease workers, default is False, which means it will always decreased the most recent worker",
    dest="randomly",
        )
parser.add_argument(
    "-min_move_time",
    default=60,
    help="minimum seconds that controller takes move to INCREASE, DECREASE or STAY SAME, default is 60 sec",
        )

parser.add_argument(
    "-max_move_time",
    default= 120,
    help="maximum seconds that controller takes move to INCREASE, DECREASE or STAY SAME, default is 120 sec",
)

class SimpleElasticController:

    def __init__(self, args):
        self.job_name = args.job_name
        self.namespace = args.namespace
        self.min_worker = args.min_worker
        self.max_worker = args.max_worker
        self.min_move_time = args.min_move_time
        self.max_move_time = args.max_move_time
        self.randomly = args.randomly
        self._idxes = [i for i in range(self.max_worker)]
        self.worker_size = 0
        # k8s client config
        config.load_kube_config()
        
        
    
    def next_move_time(self):
        return time.time() + random.randint(self.min_move_time, self.max_move_time) 

    def should_stop(self):
        api = client.CoreV1Api()
        pod = api.read_namespaced_pod_status(self._get_name(self._idxes[0]), self.namespace)
        
        return pod.status.phase == "Succeeded"

    def next_move(self):
        return random.choice([INCREASE, STAY, DECREASE])

    def run(self):
        self.create_workers()
        print("Begin with worker nums: ", self.worker_size)
        next_time =  self.next_move_time()
        while True:
            if self.should_stop():
                print("Successed, exit")
                break
            if time.time() > next_time:
                move = self.next_move()
                next_time = self.next_move_time()
                print("Move time")
                if move == DECREASE and self.worker_size > self.min_worker:
                    print("DECREASE")
                    self.delete_worker()
                elif move == INCREASE and self.worker_size < self.max_worker:
                    print("INCREASE")
                    self.add_worker()
            
                else:
                    print("STAY")

    def create_workers(self):
        self.worker_size = random.randint(self.min_worker, self.max_worker)
        for i in range(self.worker_size):
            self._create_k8s_worker(self._idxes[i])
            

    def delete_worker(self):
        try:
            if self.randomly:
                idx = random.randint(0, self.worker_size - 1)
                self._idxes[idx], self._idxes[self.worker_size - 1] = self._idxes[self.worker_size - 1], self._idxes[idx]
            self._delete_k8s_worker(self._idxes[self.worker_size - 1])
            self.worker_size -= 1
        except Exception as e:
            print("delete worker failed", e)

    def add_worker(self):
        try:
            self._create_k8s_worker(self._idxes[self.worker_size])
            self.worker_size += 1
        except Exception as e:
            print("add worker failed", e)
        
        

    def _load_worker_tpl(self):
        base_dir = os.path.dirname(__file__)
        with open(base_dir +"/imagenet-worker.tpl") as f:
            return list(yaml.safe_load_all(f))

    def _get_name(self, i):
        return self.job_name + '-' + str(i)

    def _create_k8s_worker(self, i):

        # hardcode for the sake of simlicity
        pod, svc = self._load_worker_tpl()
        pod['metadata']['name'] = self._get_name(i)
        pod['metadata']['namespace'] = self.namespace
        pod['metadata']['labels']['job-name'] = self.job_name 
        pod['metadata']['labels']['worker'] = str(i)
        
        for data in pod['spec']['containers'][0]['env']:
            if data['name'] == "JOB_ID":
                data['value'] = self.job_name
            elif data['name'] == "MIN_SIZE":
                data['value'] = str(self.min_worker)
            elif data['name'] == "MAX_SIZE":
                data['value'] = str(self.max_worker)
        
        svc['metadata']['name'] = self._get_name(i)
        svc['metadata']['namespace'] = self.namespace
        svc['metadata']['labels']['job-name'] = self.job_name
        svc['spec']['selector']['job-name'] = self.job_name
        svc['spec']['selector']['worker'] = str(i)

        api = client.CoreV1Api()
        api.create_namespaced_pod(namespace=self.namespace, body=pod)
        api.create_namespaced_service(namespace=self.namespace, body=svc)



    def _delete_k8s_worker(self, i):
        api = client.CoreV1Api()
        api.delete_namespaced_pod(name=self._get_name(i), namespace=self.namespace)
        api.delete_namespaced_service(name=self._get_name(i), namespace=self.namespace)



if __name__ == "__main__":
    args = parser.parse_args()
    controller = SimpleElasticController(args)
    controller.run()
