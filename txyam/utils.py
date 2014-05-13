from twisted.internet import defer


def deferredDict(d):
    """
    Just like a C{defer.DeferredList} but instead accepts and returns a
    C{dict}.

    @param d: A C{dict} whose values are all C{Deferred} objects.

    @return: A C{DeferredList} whose callback will be given a dictionary whose
    keys are the same as the parameter C{d}'s and whose values are the results
    of each individual deferred call.
    """
    if len(d) == 0:
        return defer.succeed({})

    def handle(results, names):
        rvalue = {}
        for key, (succeeded, value) in zip(names, results):
            if succeeded:
                rvalue[key] = value
        return rvalue

    dl = defer.DeferredList(d.values(), consumeErrors=True)
    return dl.addCallback(handle, d.keys())
