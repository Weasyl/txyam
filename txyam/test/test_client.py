import pytest
from twisted.internet.error import ConnectionAborted
from twisted.internet import defer
from twisted.python.failure import Failure
from twisted.test import proto_helpers
from twisted.trial.unittest import TestCase

from txyam import client


class FakeEndpoint(object):
    def __init__(self, failure=None):
        self.failure = failure
        self.deferred = None

    def _abortConnection(self):
        self.aborted.append(True)
        self.proto.connectionLost(Failure(ConnectionAborted()))

    def connect(self, fac):
        self.factory = fac
        self.proto = fac.buildProtocol(None)
        transport = proto_helpers.StringTransport()
        self.aborted = []
        transport.abortConnection = self._abortConnection
        self.proto.makeConnection(transport)
        self.transport = transport
        if self.deferred:
            return self.deferred
        if self.failure:
            return defer.fail(self.failure)
        return defer.succeed(self.proto)


class FakeError(Exception):
    pass


@pytest.fixture
def clock():
    return proto_helpers.Clock()


@pytest.fixture
def yam(clock, **kw):
    endpoints = {
        'fake:1': FakeEndpoint(),
        'fake:2': FakeEndpoint(),
    }

    def clientFromString(reactor, description):
        assert reactor is clock
        return endpoints[description]

    yam = client.YamClient(clock, ['fake:1', 'fake:2'], **kw)
    yam._endpoints = endpoints
    yam.clientFromString = clientFromString
    return yam


class YamClientTests(TestCase):
    @pytest.fixture(autouse=True)
    def _init(self, clock, yam):
        self.clock = clock
        self.yam = yam

    def test_connectionDeferred(self):
        """
        The deferred returned by YamClient.connect waits until all clients'
        deferreds have fired before firing itself.
        """
        endpoint = self.yam._endpoints['fake:1']
        endpoint.deferred = defer.Deferred()
        d = self.yam.connect()
        self.assertNoResult(d)
        endpoint.deferred.callback(endpoint.proto)
        self.assertIs(self.successResultOf(d), self.yam)

    def test_logConnectionFailures(self):
        """
        Connection failures get logged.
        """
        endpoint = self.yam._endpoints['fake:1']
        endpoint.failure = FakeError()
        self.yam.connect()
        self.assertEqual(len(self.flushLoggedErrors(FakeError)), 1)

    def test_logConnectionLosses(self):
        """
        Connection losses get logged.
        """
        endpoint = self.yam._endpoints['fake:1']
        self.yam.connect()
        endpoint.proto.connectionLost(Failure(FakeError()))
        self.assertEqual(len(self.flushLoggedErrors(FakeError)), 1)

    def test_retryOnConnectionFailure(self):
        """
        On a connection failure, the connection is reattempted after the retry
        delay.
        """
        endpoint = self.yam._endpoints['fake:1']
        endpoint.failure = FakeError()
        self.yam.connect()
        self.clock.advance(2)
        self.assertEqual(len(self.flushLoggedErrors(FakeError)), 2)

    def test_retryOnConnectionFailureTwice(self):
        """
        Reattempts happen more than just once.
        """
        endpoint = self.yam._endpoints['fake:1']
        endpoint.failure = FakeError()
        self.yam.connect()
        self.clock.advance(2)
        self.clock.advance(2)
        self.assertEqual(len(self.flushLoggedErrors(FakeError)), 3)

    def test_retryOnConnectionLoss(self):
        """
        On a connection loss, the connection is reattempted after the retry
        delay.
        """
        endpoint = self.yam._endpoints['fake:1']
        self.yam.connect()
        proto = endpoint.proto
        endpoint.proto.connectionLost(Failure(FakeError()))
        self.clock.advance(2)
        self.assertIsNot(proto, endpoint.proto)
        self.assertEqual(len(self.flushLoggedErrors(FakeError)), 1)

    def test_retryOnConnectionLossTwice(self):
        """
        Reattempts happen more than just once.
        """
        endpoint = self.yam._endpoints['fake:1']
        self.yam.connect()
        proto1 = endpoint.proto
        proto1.connectionLost(Failure(FakeError()))
        self.clock.advance(2)
        proto2 = endpoint.proto
        proto2.connectionLost(Failure(FakeError()))
        self.clock.advance(2)
        self.assertNotIn(endpoint.proto, (proto1, proto2))
        self.assertEqual(len(self.flushLoggedErrors(FakeError)), 2)

    def test_retryOnConnectionAborted(self):
        """
        On a connection loss from a ConnectionAborted failure, the connection
        is reattempted immediately.
        """
        endpoint = self.yam._endpoints['fake:1']
        self.yam.connect()
        proto = endpoint.proto
        endpoint.proto.connectionLost(Failure(ConnectionAborted()))
        self.assertIsNot(proto, endpoint.proto)
        self.assertEqual(len(self.flushLoggedErrors(ConnectionAborted)), 1)

    def test_disconnectCancelsConnectingDeferreds(self):
        """
        Any deferreds from connections still connecting get canceled on calling
        .disconnect().
        """
        endpoint = self.yam._endpoints['fake:1']
        canceled = []
        endpoint.deferred = defer.Deferred(canceled.append)
        self.yam.connect()
        self.yam.disconnect()
        self.assertEqual(canceled, [endpoint.deferred])

    def test_noConnectionFailureLoggingAfterDisconnection(self):
        """
        Connection failures don't get logged if .disconnect() has been called.
        """
        endpoint = self.yam._endpoints['fake:1']
        endpoint.deferred = defer.Deferred(lambda d: d.errback(FakeError()))
        self.yam.connect()
        self.yam.disconnect()
        self.assertEqual(len(self.flushLoggedErrors(FakeError)), 0)

    def test_noConnectionLossLoggingAfterDisconnection(self):
        """
        Connection losses don't get logged if .disconnect() has been called.
        """
        endpoint = self.yam._endpoints['fake:1']
        self.yam.connect()
        self.yam.disconnect()
        endpoint.proto.connectionLost(Failure(FakeError()))
        self.assertEqual(len(self.flushLoggedErrors(FakeError)), 0)

    def test_flushAllQuery(self):
        """
        flushAll sends a flush_all to every connected client.
        """
        self.yam.connect()
        self.yam.flushAll()
        ep1 = self.yam._endpoints['fake:1']
        ep2 = self.yam._endpoints['fake:2']
        self.assertEqual(ep1.transport.value(), 'flush_all\r\n')
        self.assertEqual(ep2.transport.value(), 'flush_all\r\n')

    def test_flushAllAnswer(self):
        """
        flushAll fires with a list of results; one element per client.
        """
        self.yam.connect()
        d = self.yam.flushAll()
        self.assertNoResult(d)
        ep1 = self.yam._endpoints['fake:1']
        ep2 = self.yam._endpoints['fake:2']
        ep1.proto.dataReceived('OK\r\n')
        ep2.proto.dataReceived('OK\r\n')
        self.assertEqual(self.successResultOf(d), [True, True])

    def test_flushAllAnswerWithOneClient(self):
        """
        flushAll will only include a number of elements equal to the number of
        connected clients.
        """
        self.yam._endpoints['fake:1'].failure = FakeError()
        self.yam.connect()
        self.assertEqual(len(self.flushLoggedErrors(FakeError)), 1)
        d = self.yam.flushAll()
        self.assertNoResult(d)
        ep2 = self.yam._endpoints['fake:2']
        ep2.proto.dataReceived('OK\r\n')
        self.assertEqual(self.successResultOf(d), [True])

    def test_flushAllAnswerWithNoClients(self):
        """
        If there are no clients available, flushAll immediately fires with an
        empty list.
        """
        self.yam._endpoints['fake:1'].failure = FakeError()
        self.yam._endpoints['fake:2'].failure = FakeError()
        self.yam.connect()
        self.assertEqual(len(self.flushLoggedErrors(FakeError)), 2)
        d = self.yam.flushAll()
        self.assertEqual(self.successResultOf(d), [])

    def test_statsQuery(self):
        """
        stats sends a stats query to every connected client.
        """
        self.yam.connect()
        self.yam.stats()
        ep1 = self.yam._endpoints['fake:1']
        ep2 = self.yam._endpoints['fake:2']
        self.assertEqual(ep1.transport.value(), 'stats\r\n')
        self.assertEqual(ep2.transport.value(), 'stats\r\n')

    def test_statsAnswer(self):
        """
        stats aggregates responses by endpoint string description.
        """
        self.yam.connect()
        d = self.yam.stats()
        self.assertNoResult(d)
        ep1 = self.yam._endpoints['fake:1']
        ep2 = self.yam._endpoints['fake:2']
        ep1.proto.dataReceived('STAT key1 value1\r\nEND\r\n')
        ep2.proto.dataReceived('STAT key2 value2\r\nEND\r\n')
        self.assertEqual(
            self.successResultOf(d),
            {
                'fake:1': {'key1': 'value1'},
                'fake:2': {'key2': 'value2'},
            })

    def test_statsAnswerWithOneClient(self):
        """
        stats will only provide answers for the connected clients.
        """
        self.yam._endpoints['fake:1'].failure = FakeError()
        self.yam.connect()
        self.assertEqual(len(self.flushLoggedErrors(FakeError)), 1)
        d = self.yam.stats()
        self.assertNoResult(d)
        ep2 = self.yam._endpoints['fake:2']
        ep2.proto.dataReceived('STAT key2 value2\r\nEND\r\n')
        self.assertEqual(
            self.successResultOf(d),
            {
                'fake:2': {'key2': 'value2'},
            })

    def test_statsAnswerWithNoClients(self):
        """
        If there are no clients available, stats immediately fires with an
        empty dict.
        """
        self.yam._endpoints['fake:1'].failure = FakeError()
        self.yam._endpoints['fake:2'].failure = FakeError()
        self.yam.connect()
        self.assertEqual(len(self.flushLoggedErrors(FakeError)), 2)
        d = self.yam.stats()
        self.assertEqual(self.successResultOf(d), {})

    def test_versionQuery(self):
        """
        version sends a version query to every connected client.
        """
        self.yam.connect()
        self.yam.version()
        ep1 = self.yam._endpoints['fake:1']
        ep2 = self.yam._endpoints['fake:2']
        self.assertEqual(ep1.transport.value(), 'version\r\n')
        self.assertEqual(ep2.transport.value(), 'version\r\n')

    def test_versionAnswer(self):
        """
        version aggregates responses by endpoint string description.
        """
        self.yam.connect()
        d = self.yam.version()
        self.assertNoResult(d)
        ep1 = self.yam._endpoints['fake:1']
        ep2 = self.yam._endpoints['fake:2']
        ep1.proto.dataReceived('VERSION 1.1.1\r\n')
        ep2.proto.dataReceived('VERSION 2.2.2\r\n')
        self.assertEqual(
            self.successResultOf(d),
            {
                'fake:1': '1.1.1',
                'fake:2': '2.2.2',
            })

    def test_versionAnswerWithOneClient(self):
        """
        version will only provide answers for the connected clients.
        """
        self.yam._endpoints['fake:1'].failure = FakeError()
        self.yam.connect()
        self.assertEqual(len(self.flushLoggedErrors(FakeError)), 1)
        d = self.yam.version()
        self.assertNoResult(d)
        ep2 = self.yam._endpoints['fake:2']
        ep2.proto.dataReceived('VERSION 2.2.2\r\n')
        self.assertEqual(
            self.successResultOf(d),
            {
                'fake:2': '2.2.2',
            })

    def test_versionAnswerWithNoClients(self):
        """
        If there are no clients available, version immediately fires with an
        empty dict.
        """
        self.yam._endpoints['fake:1'].failure = FakeError()
        self.yam._endpoints['fake:2'].failure = FakeError()
        self.yam.connect()
        self.assertEqual(len(self.flushLoggedErrors(FakeError)), 2)
        d = self.yam.version()
        self.assertEqual(self.successResultOf(d), {})

    def test_getMultipleQuery(self):
        """
        getMultiple issues queries to multiple clients.
        """
        self.yam.connect()
        self.yam.getMultiple(['key1', 'key2', 'key3', 'key4', 'key5'])
        ep1 = self.yam._endpoints['fake:1']
        ep2 = self.yam._endpoints['fake:2']
        self.assertEqual(ep1.transport.value(), 'get key5\r\n')
        self.assertEqual(ep2.transport.value(), 'get key1 key2 key3 key4\r\n')

    def test_getMultipleAnswer(self):
        """
        getMultiple aggregates answers from each client.
        """
        self.yam.connect()
        d = self.yam.getMultiple(['key1', 'key2', 'key3', 'key4', 'key5'])
        self.assertNoResult(d)
        ep1 = self.yam._endpoints['fake:1']
        ep2 = self.yam._endpoints['fake:2']
        ep1.proto.dataReceived('VALUE key5 0 1\r\n5\r\nEND\r\n')
        ep2.proto.dataReceived('VALUE key1 0 1\r\n1\r\n')
        ep2.proto.dataReceived('VALUE key2 0 1\r\n2\r\n')
        ep2.proto.dataReceived('VALUE key3 0 1\r\n3\r\n')
        ep2.proto.dataReceived('VALUE key4 0 1\r\n4\r\nEND\r\n')
        self.assertEqual(
            self.successResultOf(d),
            {
                'key1': (0, '1'),
                'key2': (0, '2'),
                'key3': (0, '3'),
                'key4': (0, '4'),
                'key5': (0, '5'),
            })

    def test_getMultipleQueryWithOneClient(self):
        """
        Because of consistent hashing, if one client out of two is down, the
        other will receive all of the downed client's requests.
        """
        self.yam._endpoints['fake:2'].failure = FakeError()
        self.yam.connect()
        self.assertEqual(len(self.flushLoggedErrors(FakeError)), 1)
        self.yam.getMultiple(['key1', 'key2', 'key3', 'key4', 'key5'])
        ep1 = self.yam._endpoints['fake:1']
        self.assertEqual(
            ep1.transport.value(), 'get key1 key2 key3 key4 key5\r\n')

    def test_getMultipleQueryWithNoClients(self):
        """
        If there are no clients available, getMultiple will immediately fire
        with an empty dict.
        """
        self.yam._endpoints['fake:1'].failure = FakeError()
        self.yam._endpoints['fake:2'].failure = FakeError()
        self.yam.connect()
        self.assertEqual(len(self.flushLoggedErrors(FakeError)), 2)
        d = self.yam.getMultiple(['key1', 'key2', 'key3', 'key4', 'key5'])
        self.assertEqual(self.successResultOf(d), {})

    def test_setMultipleQuery(self):
        """
        setMultiple issues commands to multiple clients.
        """
        self.yam.connect()
        self.yam.setMultiple({
            'key1': '1', 'key2': '2', 'key3': '3', 'key4': '4', 'key5': '5',
        })
        ep1 = self.yam._endpoints['fake:1']
        ep2 = self.yam._endpoints['fake:2']
        self.assertEqual(ep1.transport.value(), 'set key5 0 0 1\r\n5\r\n')
        self.assertEqual(
            sorted(ep2.transport.value().splitlines()[::2]), [
                'set key1 0 0 1',
                'set key2 0 0 1',
                'set key3 0 0 1',
                'set key4 0 0 1',
            ])

    def test_setMultipleAnswer(self):
        """
        setMultiple aggregates answers from each client.
        """
        self.yam.connect()
        d = self.yam.setMultiple({
            'key1': '1', 'key2': '2', 'key3': '3', 'key4': '4', 'key5': '5',
        })
        self.assertNoResult(d)
        ep1 = self.yam._endpoints['fake:1']
        ep2 = self.yam._endpoints['fake:2']
        ep1.proto.dataReceived('STORED\r\n')
        ep2.proto.dataReceived('STORED\r\n' * 4)
        self.assertEqual(
            self.successResultOf(d),
            dict.fromkeys(['key1', 'key2', 'key3', 'key4', 'key5'], True))

    def test_setMultipleQueryWithOneClient(self):
        """
        Because of consistent hashing, if one client out of two is down, the
        other will receive all of the downed client's requests.
        """
        self.yam._endpoints['fake:2'].failure = FakeError()
        self.yam.connect()
        self.assertEqual(len(self.flushLoggedErrors(FakeError)), 1)
        self.yam.setMultiple({
            'key1': '1', 'key2': '2', 'key3': '3', 'key4': '4', 'key5': '5',
        })
        ep1 = self.yam._endpoints['fake:1']
        self.assertEqual(
            sorted(ep1.transport.value().splitlines()[::2]), [
                'set key1 0 0 1',
                'set key2 0 0 1',
                'set key3 0 0 1',
                'set key4 0 0 1',
                'set key5 0 0 1',
            ])

    def test_setMultipleQueryWithNoClients(self):
        """
        If there are no clients available, setMultiple will immediately fire
        with a dict mapping each key to None.
        """
        self.yam._endpoints['fake:1'].failure = FakeError()
        self.yam._endpoints['fake:2'].failure = FakeError()
        self.yam.connect()
        self.assertEqual(len(self.flushLoggedErrors(FakeError)), 2)
        d = self.yam.setMultiple({
            'key1': '1', 'key2': '2', 'key3': '3', 'key4': '4', 'key5': '5',
        })
        self.assertEqual(
            self.successResultOf(d),
            dict.fromkeys(['key1', 'key2', 'key3', 'key4', 'key5']))

    def test_deleteMultipleQuery(self):
        """
        deleteMultiple issues commands to multiple clients.
        """
        self.yam.connect()
        self.yam.deleteMultiple(['key1', 'key2', 'key3', 'key4', 'key5'])
        ep1 = self.yam._endpoints['fake:1']
        ep2 = self.yam._endpoints['fake:2']
        self.assertEqual(ep1.transport.value(), 'delete key5\r\n')
        self.assertEqual(
            sorted(ep2.transport.value().splitlines()),
            ['delete key1', 'delete key2', 'delete key3', 'delete key4'])

    def test_deleteMultipleAnswer(self):
        """
        deleteMultiple aggregates answers from each client.
        """
        self.yam.connect()
        d = self.yam.deleteMultiple(['key1', 'key2', 'key3', 'key4', 'key5'])
        self.assertNoResult(d)
        ep1 = self.yam._endpoints['fake:1']
        ep2 = self.yam._endpoints['fake:2']
        ep1.proto.dataReceived('DELETED\r\n')
        ep2.proto.dataReceived('DELETED\r\n' * 4)
        self.assertEqual(
            self.successResultOf(d),
            dict.fromkeys(['key1', 'key2', 'key3', 'key4', 'key5'], True))

    def test_deleteMultipleQueryWithOneClient(self):
        """
        Because of consistent hashing, if one client out of two is down, the
        other will receive all of the downed client's requests.
        """
        self.yam._endpoints['fake:2'].failure = FakeError()
        self.yam.connect()
        self.assertEqual(len(self.flushLoggedErrors(FakeError)), 1)
        self.yam.deleteMultiple(['key1', 'key2', 'key3', 'key4', 'key5'])
        ep1 = self.yam._endpoints['fake:1']
        self.assertEqual(
            sorted(ep1.transport.value().splitlines()),
            ['delete key1', 'delete key2', 'delete key3', 'delete key4',
             'delete key5'])

    def test_deleteMultipleQueryWithNoClients(self):
        """
        If there are no clients available, deleteMultiple will immediately fire
        with an empty dict.
        """
        self.yam._endpoints['fake:1'].failure = FakeError()
        self.yam._endpoints['fake:2'].failure = FakeError()
        self.yam.connect()
        self.assertEqual(len(self.flushLoggedErrors(FakeError)), 2)
        d = self.yam.deleteMultiple(['key1', 'key2', 'key3', 'key4', 'key5'])
        self.assertEqual(
            self.successResultOf(d),
            dict.fromkeys(['key1', 'key2', 'key3', 'key4', 'key5']))


class CustomYamClientTests(TestCase):
    @pytest.fixture(autouse=True)
    def _init(self, clock):
        self.clock = clock

    def test_configuringTimeout(self):
        """
        The delay before a command times out can be customized.
        """
        self.yam = yam(self.clock, timeOut=1)
        self.yam.connect()
        d = self.yam.get('key1')
        self.assertNoResult(d)
        self.clock.advance(1)
        self.assertIdentical(self.successResultOf(d), None)
        # this one is from the connection loss event
        self.assertEqual(len(self.flushLoggedErrors(ConnectionAborted)), 1)

    def test_configuringRetryDelay(self):
        """
        The delay before reconnecting can be customized.
        """
        self.yam = yam(self.clock, retryDelay=1)
        endpoint = self.yam._endpoints['fake:1']
        endpoint.failure = FakeError()
        self.yam.connect()
        self.clock.advance(1)
        self.assertEqual(len(self.flushLoggedErrors(FakeError)), 2)


class YamClientCommandTestsMixin(object):
    @pytest.fixture(autouse=True)
    def _init(self, clock, yam):
        self.clock = clock
        self.yam = yam

    method = None
    arguments = ()
    query = None
    response = None
    deferredResult = None

    def test_query(self):
        """
        The command will be issued to the appropriate client.
        """
        self.yam.connect()
        getattr(self.yam, self.method)('key1', *self.arguments)
        ep = self.yam._endpoints['fake:2']
        self.assertEqual(ep.transport.value(), self.query)

    def test_answer(self):
        """
        The command's deferred fires only when the appropriate response is
        given.
        """
        self.yam.connect()
        d = getattr(self.yam, self.method)('key1', *self.arguments)
        self.assertNoResult(d)
        ep = self.yam._endpoints['fake:2']
        ep.proto.dataReceived(self.response)
        self.assertEqual(self.successResultOf(d), self.deferredResult)

    def test_answerWithClientMissing(self):
        """
        The command will fail over to the other client if the usual client is
        missing.
        """
        self.yam._endpoints['fake:2'].failure = FakeError()
        self.yam.connect()
        self.assertEqual(len(self.flushLoggedErrors(FakeError)), 1)
        getattr(self.yam, self.method)('key1', *self.arguments)
        ep = self.yam._endpoints['fake:1']
        self.assertEqual(ep.transport.value(), self.query)

    def test_answerWithBothClientsMissing(self):
        """
        The command will silently fire with None if no clients are connected.
        """
        self.yam._endpoints['fake:1'].failure = FakeError()
        self.yam._endpoints['fake:2'].failure = FakeError()
        self.yam.connect()
        self.assertEqual(len(self.flushLoggedErrors(FakeError)), 2)
        d = getattr(self.yam, self.method)('key1', *self.arguments)
        self.assertIdentical(self.successResultOf(d), None)

    def test_timedOut(self):
        """
        The command will silently fire with None if the command times out.
        """
        self.yam.connect()
        d = getattr(self.yam, self.method)('key1', *self.arguments)
        self.assertNoResult(d)
        self.clock.advance(60)
        self.assertIdentical(self.successResultOf(d), None)
        # this one is from the connection loss event
        self.assertEqual(len(self.flushLoggedErrors(ConnectionAborted)), 1)

    def test_multipleCommandsTimedOut(self):
        """
        All commands will silently fire with None when the first command times
        out.
        """
        self.yam.connect()
        d1 = getattr(self.yam, self.method)('key1', *self.arguments)
        self.assertNoResult(d1)
        self.clock.advance(30)
        d2 = getattr(self.yam, self.method)('key1', *self.arguments)
        self.assertNoResult(d2)
        self.clock.advance(30)
        self.assertIdentical(self.successResultOf(d1), None)
        self.assertIdentical(self.successResultOf(d2), None)
        # as before, this one is from the connection loss event
        self.assertEqual(len(self.flushLoggedErrors(ConnectionAborted)), 1)


class YamClientCommandSetTests(TestCase, YamClientCommandTestsMixin):
    method = 'set'
    arguments = ('value',)
    query = 'set key1 0 0 5\r\nvalue\r\n'
    response = 'STORED\r\n'
    deferredResult = True


class YamClientCommandGetTests(TestCase, YamClientCommandTestsMixin):
    method = 'get'
    query = 'get key1\r\n'
    response = 'VALUE key1 0 1\r\nx\r\nEND\r\n'
    deferredResult = (0, 'x')


class YamClientCommandIncrementTests(TestCase, YamClientCommandTestsMixin):
    method = 'increment'
    query = 'incr key1 1\r\n'
    response = '2\r\n'
    deferredResult = 2


class YamClientCommandDecrementTests(TestCase, YamClientCommandTestsMixin):
    method = 'decrement'
    query = 'decr key1 1\r\n'
    response = '2\r\n'
    deferredResult = 2


class YamClientCommandReplaceTests(TestCase, YamClientCommandTestsMixin):
    method = 'replace'
    arguments = ('value',)
    query = 'replace key1 0 0 5\r\nvalue\r\n'
    response = 'STORED\r\n'
    deferredResult = True


class YamClientCommandAddTests(TestCase, YamClientCommandTestsMixin):
    method = 'add'
    arguments = ('value',)
    query = 'add key1 0 0 5\r\nvalue\r\n'
    response = 'STORED\r\n'
    deferredResult = True


class YamClientCommandCheckAndSetTests(TestCase, YamClientCommandTestsMixin):
    method = 'checkAndSet'
    arguments = ('value', '9')
    query = 'cas key1 0 0 5 9\r\nvalue\r\n'
    response = 'STORED\r\n'
    deferredResult = True


class YamClientCommandAppendTests(TestCase, YamClientCommandTestsMixin):
    method = 'append'
    arguments = ('value',)
    query = 'append key1 0 0 5\r\nvalue\r\n'
    response = 'STORED\r\n'
    deferredResult = True


class YamClientCommandPrependTests(TestCase, YamClientCommandTestsMixin):
    method = 'prepend'
    arguments = ('value',)
    query = 'prepend key1 0 0 5\r\nvalue\r\n'
    response = 'STORED\r\n'
    deferredResult = True


class YamClientCommandDeleteTests(TestCase, YamClientCommandTestsMixin):
    method = 'delete'
    query = 'delete key1\r\n'
    response = 'DELETED\r\n'
    deferredResult = True
