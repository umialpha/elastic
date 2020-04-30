import threading
import functools

class ElasticDDP:

    def __init__(self, model, optimizer, current_batch, current_epoch):
        self.model = model
        self.optimizer = optimizer
        self.current_batch = current_batch
        self.current_epoch = current_epoch
        self.notification_store = None
        self._pre_states = {}
        

    def save(self):
        return {}

    def reset_save_count(self):
        pass

    def _should_save(self):
        return True

    def commit(self):
        if self.notification_store is None:
            return
        if self._should_save():
            self._pre_states = self.save()
            self.reset_save_count()
        
        notification = self.notification_store.read_notification()
        if notification == "MemberChange":
            raise MemberChangeException()


class NotificationStore:

    def wait_group_ready(self):
        raise NotImplementedError()

    def read_notification(self):
        raise NotImplementedError()


class EtcdNotificationStore:

    def __init__(self):
        pass




def elastic_run(func):
    @functools.wraps(func)
    def wrapper(model, *args, **kwargs):
        assert(model is ElasticDDP)
        model.notification_store = NotificationStore(...)
        model.notification_store.wait_group_ready()
        model.init_process_group()
        reset_required = False
        while True:
            if reset_required:
                model.on_reset()
                model.reinitialize_process_group()
                reset_required = False
            model.sync()
            try:
                return func(model, *args, **kwargs)
            # some member failed to AllReduce
            except AllReduceException:
                model.restore()
            except MemberChangeException:
                pass
            reset_required = True
    return wrapper

            