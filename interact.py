#!/usr/bin/env python

from twisted.internet.defer import inlineCallbacks, DeferredList
from twisted.internet.endpoints import ProcessEndpoint
from twisted.internet.protocol import Factory
from twisted.protocols import amp

class Sum(amp.Command):
    arguments = [(b'a', amp.Integer()),
                 (b'b', amp.Integer())]
    response = [(b'total', amp.Integer())]

class SumMany(amp.Command):
    arguments = [(b'ops', amp.AmpList([(b'a', amp.Integer()),
                                       (b'b', amp.Integer())]))]
    response = [(b'totals', amp.AmpList([(b'total', amp.Integer())]))]


class Responder(amp.AMP):
    def makeConnection(self, transport):
        transport.getPeer = lambda: 'local'
        transport.getHost = lambda: 'host'
        super().makeConnection(transport)

    @Sum.responder
    def do_sum(self, a, b):
        total = a + b
        print(f'Did a sum: {a} + {b} = {total}')
        return {'total': total}

    @SumMany.responder
    def do_sums(self, ops):
        totals = [{'total': o['a'] + o['b']} for o in ops]
        return {'totals': totals}
    
RUST_PATH = 'target/debug/amp-test'

@inlineCallbacks
def start():
    from twisted.internet import reactor

    ep = ProcessEndpoint(reactor, RUST_PATH, [RUST_PATH], childFDs={0: 'w', 1: 'r', 2: 2}, env=None)
    factory = Factory.forProtocol(Responder)
    rust = yield ep.connect(factory)


    call1 = rust.callRemote(Sum, a=1, b=2)
    call2 = rust.callRemote(Sum, a=4, b=-100)

    dl = yield DeferredList([call1, call2])
    print(dl)

    reactor.stop()

def main():
    from twisted.internet import reactor

    start().addBoth(lambda result: print(f"result: {result!r}"))

    print("Reactor running")
    reactor.run()

main()
