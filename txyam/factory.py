from twisted.internet.defer import Deferred
from twisted.internet.protocol import Factory
from twisted.protocols.memcache import MemCacheProtocol


class ConnectingMemCacheProtocol(MemCacheProtocol):
    def __init__(self):
        MemCacheProtocol.__init__(self)
        self.deferred = Deferred()

    def connectionLost(self, reason):
        MemCacheProtocol.connectionLost(self, reason)
        self.deferred.errback(reason)


class MemCacheClientFactory(Factory):
    protocol = ConnectingMemCacheProtocol
