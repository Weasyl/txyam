from twisted.internet.defer import Deferred
from twisted.internet.protocol import Factory
from twisted.protocols.memcache import MemCacheProtocol


class ConnectingMemCacheProtocol(MemCacheProtocol):
    def __init__(self, factory, reactor, **kw):
        MemCacheProtocol.__init__(self, **kw)
        self.factory = factory
        self.reactor = reactor
        self.deferred = Deferred()

    def callLater(self, *a, **kw):
        return self.reactor.callLater(*a, **kw)

    def timeoutConnection(self):
        self.transport.abortConnection()

    def connectionLost(self, reason):
        MemCacheProtocol.connectionLost(self, reason)
        self.deferred.errback(reason)


class MemCacheClientFactory(Factory):
    protocol = ConnectingMemCacheProtocol

    def __init__(self, *a, **kw):
        self._protocolArgs = a
        self._protocolKwargs = kw

    def buildProtocol(self, addr):
        return self.protocol(self, *self._protocolArgs, **self._protocolKwargs)
