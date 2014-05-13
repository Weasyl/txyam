from collections import defaultdict

from consistent_hash.consistent_hash import ConsistentHash
from twisted.internet import defer, endpoints
from twisted.python import log

from txyam.utils import deferredDict
from txyam.factory import MemCacheClientFactory


def _wrap(cmd):
    """
    Used to wrap all of the memcache methods (get,set,getMultiple,etc).
    """
    def wrapper(self, key, *args, **kwargs):
        client = self.getClient(key)
        if client is None:
            return None
        func = getattr(client, cmd)
        return func(key, *args, **kwargs)
    return wrapper


class YamClient(object):
    def __init__(self, hosts, connect=True, reactor=None, retryDelay=2):
        if reactor is None:
            from twisted.internet import reactor

        self._allHosts = hosts
        self._consistentHash = ConsistentHash([])
        self._connectionDeferreds = set()
        self._protocols = {}
        self._retryDelay = retryDelay
        self.reactor = reactor
        self.disconnecting = False
        if connect:
            self.connect()

    def connect(self):
        self.disconnecting = False
        deferreds = []
        for host in self._allHosts:
            deferreds.append(self._connectHost(host))

        dl = defer.DeferredList(deferreds)
        dl.addCallback(lambda ign: self)
        return dl

    def _connectHost(self, host):
        endpoint = endpoints.clientFromString(self.reactor, host)
        d = endpoint.connect(MemCacheClientFactory())
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
        self.reactor.callLater(self._retryDelay, self._connectHost, host)

    @property
    def _allConnections(self):
        return self._protocols.itervalues()

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
        for host, proto in self._protocols.iteritems():
            ds[host] = proto.stats(arg)
        return deferredDict(ds)

    def version(self):
        ds = {}
        for host, proto in self._protocols.iteritems():
            ds[host] = proto.version()
        return deferredDict(ds)

    def getClient(self, key):
        return self._protocols[self._consistentHash.get_node(key)]

    def getMultiple(self, keys, withIdentifier=False):
        clients = defaultdict(list)
        for key in keys:
            clients[self.getClient(key)].append(key)
        dl = defer.DeferredList(
            [c.getMultiple(ks) for c, ks in clients.iteritems()])
        dl.addCallback(self._consolidateMultiple)
        return dl

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


def ConnectedYamClient(hosts):
    return YamClient(hosts, connect=False).connect()
