import cPickle
import zlib

from twisted.internet.defer import inlineCallbacks, DeferredList, returnValue
from twisted.internet import reactor
from twisted.python import log

from txyam.utils import ketama, deferredDict
from txyam.factory import MemCacheClientFactory


class NoServerError(Exception):
    """
    No available connected servers to accept command.
    """


class InvalidHostPortError(Exception):
    """
    Invalid host/port specification.
    """


def _wrap(cmd):
    """
    Used to wrap all of the memcache methods (get,set,getMultiple,etc).
    """
    def unwrap(result):
        return result[None]

    def wrapper(self, key, *args, **kwargs):
        client = self.getClient(key)
        request = {client: (None, cmd, (key,) + args, kwargs)}
        return self._issueRequest(request).addCallback(unwrap)
    return wrapper


def _issueRequest(request):
    """
    Issue a named request to some clients.

    This is primarily for testing purposes, so that wrappers can build up a
    request and the tests can inspect the request.
    """
    ret = {}
    for client, (resultKey, method, args, kwargs) in request.iteritems():
        method = getattr(client, method)
        ret[resultKey] = method(*args, **kwargs)
    return deferredDict(ret)


class YamClient:
    _issueRequest = staticmethod(_issueRequest)

    def __init__(self, hosts, connect=True):
        """
        @param hosts: A C{list} of C{tuple}s containing hosts and ports.
        """
        self.hosts = hosts
        if connect:
            self.connect()

    def getActiveConnections(self):
        return [factory.client for factory in self.factories
                if not factory.client is None]

    def getClient(self, key):
        hosts = self.getActiveConnections()
        log.msg("Using %i active hosts" % len(hosts))
        if len(hosts) == 0:
            raise NoServerError("No connected servers remaining.")
        return hosts[ketama(key) % len(hosts)]

    @inlineCallbacks
    def connect(self):
        self.factories = []
        for hp in self.hosts:
            if isinstance(hp, tuple):
                host, port = hp
            elif isinstance(hp, str):
                host = hp
                port = 11211
            else:
                raise InvalidHostPortError(
                    "Connection info should be either hostnames or "
                    "host/port tuples")
            factory = MemCacheClientFactory()
            reactor.connectTCP(host, port, factory)
            self.factories.append(factory)

        # fire callback when all connections have been established
        yield DeferredList([factory.deferred for factory in self.factories])
        returnValue(self)

    def disconnect(self):
        log.msg("Disconnecting from all clients.")
        for factory in self.factories:
            factory.stopTrying()
        for connection in self.getActiveConnections():
            connection.transport.loseConnection()

    def flushAll(self):
        hosts = self.getActiveConnections()
        log.msg("Flushing %i hosts" % len(hosts))
        return DeferredList([host.flushAll() for host in hosts])

    def stats(self, arg=None):
        ds = {}
        for factory in self.factories:
            if not factory.client is None:
                hp = "%s:%i" % (factory.addr.host, factory.addr.port)
                ds[hp] = factory.client.stats(arg)
        log.msg("Getting stats on %i hosts" % len(ds))
        return deferredDict(ds)

    def version(self):
        ds = {}
        for factory in self.factories:
            if not factory.client is None:
                hp = "%s:%i" % (factory.addr.host, factory.addr.port)
                ds[hp] = factory.client.version()
        log.msg("Getting version on %i hosts" % len(ds))
        return deferredDict(ds)

    def pickle(self, value, compress):
        p = cPickle.dumps(value, cPickle.HIGHEST_PROTOCOL)
        if compress:
            p = zlib.compress(p)
        return p

    def unpickle(self, value, uncompress):
        if uncompress:
            value = zlib.decompress(value)
        return cPickle.loads(value)

    def setPickled(self, key, value, **kwargs):
        value = self.pickle(value, kwargs.pop('compress', False))
        return self.set(key, value, **kwargs)

    def addPickled(self, key, value, **kwargs):
        value = self.pickle(value, kwargs.pop('compress', False))
        return self.add(key, value, **kwargs)

    def getPickled(self, key, **kwargs):
        def handleResult(result, uncompress):
            index = len(result) - 1
            result = list(result)
            if result[index] is not None:
                result[index] = self.unpickle(result[index], uncompress)
            return tuple(result)
        uncompress = kwargs.pop('uncompress', False)
        return self.get(key, **kwargs).addCallback(handleResult, uncompress)

    # Following methods can be found at
    # http://twistedmatrix.com/trac/browser/tags/releases/twisted-12.0.0/twisted/protocols/memcache.py
    set = _wrap("set")
    get = _wrap("get")
    increment = _wrap("increment")
    decrement = _wrap("decrement")
    replace = _wrap("replace")
    add = _wrap("add")
    set = _wrap("set")
    checkAndSet = _wrap("checkAndSet")
    append = _wrap("append")
    prepend = _wrap("prepend")
    getMultiple = _wrap("getMultiple")
    delete = _wrap("delete")


def ConnectedYamClient(hosts):
    return YamClient(hosts, connect=False).connect()
