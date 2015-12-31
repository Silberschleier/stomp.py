import os
import signal
import time
import unittest

import stomp
from stomp import exception

from stomp.test.testutils import *

class TestIPV6Send(unittest.TestCase):
    def setUp(self):
        conn = stomp.Connection11(get_ipv6_host())
        listener = TestListener('123')
        conn.set_listener('', listener)
        conn.start()
        conn.connect('admin', 'password', wait=True)
        self.conn = conn
        self.listener = listener
        self.timestamp = time.strftime('%Y%m%d%H%M%S')

    def tearDown(self):
        if self.conn:
            self.conn.disconnect(receipt=None)

    def test_ipv6(self):
        queuename = '/queue/testipv6-%s' % self.timestamp
        self.conn.subscribe(destination=queuename, id=1, ack='auto')

        self.conn.send(body='this is a test', destination=queuename, receipt='123')

        self.listener.wait_on_receipt()

        self.assert_(self.listener.connections == 1, 'should have received 1 connection acknowledgement')
        self.assert_(self.listener.messages == 1, 'should have received 1 message')
        self.assert_(self.listener.errors == 0, 'should not have received any errors')