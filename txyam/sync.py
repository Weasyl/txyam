from crochet import wait_for, run_in_reactor


class SynchronousYamClient(object):
    def __init__(self, yamClient):
        self.yamClient = yamClient

    @wait_for(timeout=5)
    def connect(self):
        return self.yamClient.connect()

    @wait_for(timeout=0.5)
    def operation(self, operation, *a, **kw):
        return getattr(self.yamClient, operation)(*a, **kw)

    @wait_for(timeout=0.1)
    def async_operation(self, operation, *a, **kw):
        getattr(self.yamClient, operation)(*a, **kw)

    @run_in_reactor
    def disconnect(self):
        self.yamClient.disconnect()
