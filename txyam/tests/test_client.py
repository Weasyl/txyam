import collections

from twisted.internet import defer
from twisted.python.failure import Failure
from twisted.trial import unittest

from txyam import client


class FakeClient(object):
    def __init__(self, resp=None):
        self.resp = resp

    def spam(self, a, b):
        return defer.succeed(('spam', a, b))

    def eggs(self, c, d):
        return defer.succeed(('eggs', c, d))

    def get(self):
        return self.resp


class FakeYamClient(object):
    def __init__(self):
        self.clients = {}

    def getClient(self, key):
        return self.clients.setdefault(key, FakeClient())

    def _issueRequest(self, request):
        self.request = request
        self.deferred = defer.Deferred()
        return self.deferred


FakeAddress = collections.namedtuple('FakeAddress', ['host', 'port'])


class FakeFactory(object):
    def __init__(self, host, port):
        self.addr = FakeAddress(host, port)
        self.client = FakeClient()


class IssueRequestTests(unittest.TestCase):
    def test_oneClient(self):
        """
        Issuing a request to one client will come back with one response.
        """
        c = FakeClient()
        d = client._issueRequest({c: (c, 'spam', (1,), {'b': 2})})
        self.assertEqual(
            self.successResultOf(d),
            {c: ('spam', 1, 2)})

    def test_twoClients(self):
        """
        Issuing a request to two clients will come back with two responses.
        """
        c1 = FakeClient()
        c2 = FakeClient()
        d = client._issueRequest({
            c1: (c1, 'spam', (1, 2), {}),
            c2: (c2, 'eggs', (), {'c': 3, 'd': 4}),
        })
        self.assertEqual(
            self.successResultOf(d),
            {
                c1: ('spam', 1, 2),
                c2: ('eggs', 3, 4),
            })

    def test_alternateKey(self):
        """
        A request's response can come back with a different key than the
        request was made with.
        """
        d = client._issueRequest(
            {FakeClient(): ('eggs', 'spam', (1,), {'b': 2})})
        self.assertEqual(
            self.successResultOf(d),
            {'eggs': ('spam', 1, 2)})


class WrapTests(unittest.TestCase):
    def test_wrappingSpamMethod(self):
        """
        Wrapping a method called spam will cause a request to be issued for the
        spam method.
        """
        wrapper = client._wrap('spam')
        c = FakeYamClient()
        wrapper(c, '1', '2')
        self.assertEqual(
            c.request, {c.clients['1']: (None, 'spam', ('1', '2'), {})})

    def test_wrappingEggsMethod(self):
        """
        Wrapping a method called eggs will cause a request to be issued for the
        eggs method.
        """
        wrapper = client._wrap('eggs')
        c = FakeYamClient()
        wrapper(c, '1', d='2')
        self.assertEqual(
            c.request, {c.clients['1']: (None, 'eggs', ('1',), {'d': '2'})})

    def test_unwrapping(self):
        """
        The result passed back after the request is unwrapped to a single
        value.
        """
        wrapper = client._wrap('spam')
        c = FakeYamClient()
        d = wrapper(c, '1', '2')
        self.assertNoResult(d)
        sentinel = object()
        c.deferred.callback({None: sentinel})
        self.assertIdentical(self.successResultOf(d), sentinel)


class YamClientTests(unittest.TestCase):
    def setUp(self):
        self.client = client.YamClient(['localhost'], connect=False)
        self.client._issueRequest = self._issueRequest

    def _issueRequest(self, request):
        self.request = request
        self.requestDeferred = defer.Deferred()
        return self.requestDeferred

    def test_flushAllRequestWithNoFactories(self):
        """
        If there are no factories, the flushAll request is empty.
        """
        self.client.factories = []
        self.client.flushAll()
        self.assertEqual(self.request, {})

    def test_flushAllRequestWithOneFactory(self):
        """
        flushAll requests can be issued to one factory.
        """
        fac = FakeFactory('localhost', 11211)
        self.client.factories = [fac]
        self.client.flushAll()
        self.assertEqual(self.request, {
            fac.client: (0, 'flushAll', (), {}),
        })

    def test_flushAllRequestWithTwoFactories(self):
        """
        flushAll requests can be issued to two factories.
        """
        fac1 = FakeFactory('localhost', 11211)
        fac2 = FakeFactory('localhost', 11212)
        self.client.factories = [fac1, fac2]
        self.client.flushAll()
        self.assertEqual(self.request, {
            fac1.client: (0, 'flushAll', (), {}),
            fac2.client: (1, 'flushAll', (), {}),
        })

    def test_flushAllRequestNoFactory(self):
        """
        flushAll requests aren't issued to factories without clients.
        """
        fac1 = FakeFactory('localhost', 11211)
        fac2 = FakeFactory('localhost', 11212)
        fac2.client = None
        self.client.factories = [fac1, fac2]
        self.client.flushAll()
        self.assertEqual(self.request, {
            fac1.client: (0, 'flushAll', (), {}),
        })

    def test_flushAllUnwrapping(self):
        """
        The returned deferred looks like that of a DeferredList once unwrapped.
        """
        fac = FakeFactory('localhost', 11211)
        self.client.factories = [fac]
        d = self.client.flushAll()
        self.assertNoResult(d)
        sentinel = object()
        self.requestDeferred.callback({0: sentinel})
        self.assertEqual(self.successResultOf(d), [(True, sentinel)])

    def test_flushAllUnwrappingFailure(self):
        """
        Similarly, Failures get unwrapped just like in a DeferredList.
        """
        fac = FakeFactory('localhost', 11211)
        self.client.factories = [fac]
        d = self.client.flushAll()
        self.assertNoResult(d)
        sentinel = Failure(ValueError)
        self.requestDeferred.callback({0: sentinel})
        self.assertEqual(self.successResultOf(d), [(False, sentinel)])

    def test_statsRequestWithNoFactories(self):
        """
        If there are no factories, the stats request is empty.
        """
        self.client.factories = []
        self.client.stats()
        self.assertEqual(self.request, {})

    def test_statsRequestWithOneFactory(self):
        """
        Stats requests can be issued to one factory.
        """
        fac = FakeFactory('localhost', 11211)
        self.client.factories = [fac]
        self.client.stats()
        self.assertEqual(self.request, {
            fac.client: ('localhost:11211', 'stats', (), {}),
        })

    def test_statsRequestWithTwoFactories(self):
        """
        Stats requests can be issued to two factories.
        """
        fac1 = FakeFactory('localhost', 11211)
        fac2 = FakeFactory('localhost', 11212)
        self.client.factories = [fac1, fac2]
        self.client.stats()
        self.assertEqual(self.request, {
            fac1.client: ('localhost:11211', 'stats', (), {}),
            fac2.client: ('localhost:11212', 'stats', (), {}),
        })

    def test_statsRequestNoFactory(self):
        """
        Stats requests aren't issued to factories without clients.
        """
        fac1 = FakeFactory('localhost', 11211)
        fac2 = FakeFactory('localhost', 11212)
        fac2.client = None
        self.client.factories = [fac1, fac2]
        self.client.stats()
        self.assertEqual(self.request, {
            fac1.client: ('localhost:11211', 'stats', (), {}),
        })

    def test_statsFiresWithWhatever(self):
        """
        The returned deferred will fire with whatever the request's response
        fires with.
        """
        fac = FakeFactory('localhost', 11211)
        self.client.factories = [fac]
        d = self.client.stats()
        self.assertNoResult(d)
        sentinel = object()
        self.requestDeferred.callback({'localhost:11211': sentinel})
        self.assertEqual(
            self.successResultOf(d), {'localhost:11211': sentinel})

    def test_versionRequestWithNoFactories(self):
        """
        If there are no factories, the version request is empty.
        """
        self.client.factories = []
        self.client.version()
        self.assertEqual(self.request, {})

    def test_versionRequestWithOneFactory(self):
        """
        Version requests can be issued to one factory.
        """
        fac = FakeFactory('localhost', 11211)
        self.client.factories = [fac]
        self.client.version()
        self.assertEqual(self.request, {
            fac.client: ('localhost:11211', 'version', (), {}),
        })

    def test_versionRequestWithTwoFactories(self):
        """
        Version requests can be issued to two factories.
        """
        fac1 = FakeFactory('localhost', 11211)
        fac2 = FakeFactory('localhost', 11212)
        self.client.factories = [fac1, fac2]
        self.client.version()
        self.assertEqual(self.request, {
            fac1.client: ('localhost:11211', 'version', (), {}),
            fac2.client: ('localhost:11212', 'version', (), {}),
        })

    def test_versionRequestNoFactory(self):
        """
        Version requests aren't issued to factories without clients.
        """
        fac1 = FakeFactory('localhost', 11211)
        fac2 = FakeFactory('localhost', 11212)
        fac2.client = None
        self.client.factories = [fac1, fac2]
        self.client.version()
        self.assertEqual(self.request, {
            fac1.client: ('localhost:11211', 'version', (), {}),
        })

    def test_versionFiresWithWhatever(self):
        """
        The returned deferred will fire with whatever the request's response
        fires with.
        """
        fac = FakeFactory('localhost', 11211)
        self.client.factories = [fac]
        d = self.client.version()
        self.assertNoResult(d)
        sentinel = object()
        self.requestDeferred.callback({'localhost:11211': sentinel})
        self.assertEqual(
            self.successResultOf(d), {'localhost:11211': sentinel})
