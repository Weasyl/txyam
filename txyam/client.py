from collections import defaultdict

from consistent_hash.consistent_hash import ConsistentHash
from twisted.internet.error import ConnectionAborted
from twisted.internet import defer, endpoints
from twisted.python import log

from txyam.utils import deferredDict
from txyam.factory import MemCacheClientFactory


if hasattr(dict, "iteritems"):
    def iteritems(d):
        return d.iteritems()

    def itervalues(d):
        return d.itervalues()
else:
    def iteritems(d):
        return d.items()

    def itervalues(d):
        return d.values()


def _wrap(cmd):
    """
    Used to wrap all of the memcache methods (get,set,getMultiple,etc).
    """
    def wrapper(self, key, *args, **kwargs):
        client = self.getClient(key)
        if client is None:
            return defer.succeed(None)
        func = getattr(client, cmd)
        d = func(key, *args, **kwargs)
        d.addErrback(lambda ign: None)
        return d
    return wrapper


class YamClient(object):
    clientFromString = staticmethod(endpoints.clientFromString)

    def __init__(self, reactor, hosts, retryDelay=2, **kw):
        self.reactor = reactor
        self._allHosts = hosts
        self._consistentHash = ConsistentHash([])
        self._connectionDeferreds = set()
        self._protocols = {}
        self._retryDelay = retryDelay
        self._protocolKwargs = kw
        self.disconnecting = False

    def connect(self):
        self.disconnecting = False
        deferreds = []
        for host in self._allHosts:
            deferreds.append(self._connectHost(host))

        dl = defer.DeferredList(deferreds)
        dl.addCallback(lambda ign: self)
        return dl

    def _connectHost(self, host):
        endpoint = self.clientFromString(self.reactor, host)
        d = endpoint.connect(
            MemCacheClientFactory(self.reactor, **self._protocolKwargs))
        self._connectionDeferreds.add(d)
        d.addCallback(self._gotProtocol, host, d)
        d.addErrback(self._connectionFailed, host, d)
        return d

    def _gotProtocol(self, protocol, host, deferred):
        self._connectionDeferreds.discard(deferred)
        self._protocols[host] = protocol
        self._consistentHash.add_nodes([host])
        protocol.deferred.addErrback(self._lostProtocol, host)

    def _connectionFailed(self, reason, host, deferred):
        self._connectionDeferreds.discard(deferred)
        if self.disconnecting:
            return
        log.err(reason, 'connection to %r failed' % (host,), system='txyam')
        self.reactor.callLater(self._retryDelay, self._connectHost, host)

    def _lostProtocol(self, reason, host):
        if not self.disconnecting:
            log.err(reason, 'connection to %r lost' % (host,), system='txyam')
        del self._protocols[host]
        self._consistentHash.del_nodes([host])
        if self.disconnecting:
            return
        if reason.check(ConnectionAborted):
            self._connectHost(host)
        else:
            self.reactor.callLater(self._retryDelay, self._connectHost, host)

    @property
    def _allConnections(self):
        return itervalues(self._protocols)

    def disconnect(self):
        self.disconnecting = True
        log.msg('disconnecting from all clients', system='txyam')
        for d in list(self._connectionDeferreds):
            d.cancel()
        for proto in self._allConnections:
            proto.transport.loseConnection()

    def flushAll(self):
        return defer.gatherResults(
            [proto.flushAll() for proto in self._allConnections])

    def stats(self, arg=None):
        ds = {}
        for host, proto in iteritems(self._protocols):
            ds[host] = proto.stats(arg)
        return deferredDict(ds)

    def version(self):
        ds = {}
        for host, proto in iteritems(self._protocols):
            ds[host] = proto.version()
        return deferredDict(ds)

    def getClient(self, key):
        return self._protocols.get(self._consistentHash.get_node(key))

    def getMultiple(self, keys, withIdentifier=False):
        clients = defaultdict(list)
        for key in keys:
            clients[self.getClient(key)].append(key)
        dl = defer.DeferredList(
            [c.getMultiple(ks, withIdentifier) for c, ks in iteritems(clients)
             if c is not None],
            consumeErrors=True)
        dl.addCallback(self._consolidateMultiple)
        return dl

    def setMultiple(self, items, flags=0, expireTime=0):
        ds = {}
        for key, value in iteritems(items):
            ds[key] = self.set(key, value, flags, expireTime)
        return deferredDict(ds)

    def deleteMultiple(self, keys):
        ds = {}
        for key in keys:
            ds[key] = self.delete(key)
        return deferredDict(ds)

    def _consolidateMultiple(self, results):
        ret = {}
        for succeeded, result in results:
            if succeeded:
                ret.update(result)
        return ret

    set = _wrap('set')
    get = _wrap('get')
    increment = _wrap('increment')
    decrement = _wrap('decrement')
    replace = _wrap('replace')
    add = _wrap('add')
    checkAndSet = _wrap('checkAndSet')
    append = _wrap('append')
    prepend = _wrap('prepend')
    delete = _wrap('delete')
