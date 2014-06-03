from twisted.internet import defer
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
        return defer.Deferred()


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
