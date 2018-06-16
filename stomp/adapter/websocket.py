"""Provides the underlying transport functionality (for stomp message transmission) - (mostly) independent from the actual STOMP protocol
"""

import errno
from io import BytesIO
import logging
import math
import random
import re
import socket
import sys
import threading
import time
import warnings

import traceback

try:
    import ssl
    from ssl import SSLError

    DEFAULT_SSL_VERSION = ssl.PROTOCOL_TLSv1
except (ImportError, AttributeError):  # python version < 2.6 without the backported ssl module
    ssl = None

    class SSLError(object):
        pass

    DEFAULT_SSL_VERSION = None

try:
    from socket import SOL_SOCKET, SO_KEEPALIVE
    from socket import SOL_TCP, TCP_KEEPIDLE, TCP_KEEPINTVL, TCP_KEEPCNT

    LINUX_KEEPALIVE_AVAIL = True
except ImportError:
    LINUX_KEEPALIVE_AVAIL = False

from stomp.connect import BaseConnection
from stomp.protocol import Protocol11
from stomp.backward import decode, encode, get_errno, monotonic, pack
from stomp.backwardsock import get_socket
from stomp.transport import BaseTransport
from stomp.constants import *
import stomp.exception as exception
import stomp.listener
import stomp.utils as utils

import websocket


log = logging.getLogger('stomp.py')


class WebsocketTransport(BaseTransport):
    """
    Represents a STOMP client 'transport'. Effectively this is the communications mechanism without the definition of
    the protocol.

    :param list((str,int)) host_and_ports: a list of (host, port) tuples
    :param bool prefer_localhost: if True and the local host is mentioned in the (host,
        port) tuples, try to connect to this first
    :param bool try_loopback_connect: if True and the local host is found in the host
        tuples, try connecting to it using loopback interface
        (127.0.0.1)
    :param float reconnect_sleep_initial: initial delay in seconds to wait before reattempting
        to establish a connection if connection to any of the
        hosts fails.
    :param float reconnect_sleep_increase: factor by which the sleep delay is increased after
        each connection attempt. For example, 0.5 means
        to wait 50% longer than before the previous attempt,
        1.0 means wait twice as long, and 0.0 means keep
        the delay constant.
    :param float reconnect_sleep_max: maximum delay between connection attempts, regardless
        of the reconnect_sleep_increase.
    :param float reconnect_sleep_jitter: random additional time to wait (as a percentage of
        the time determined using the previous parameters)
        between connection attempts in order to avoid
        stampeding. For example, a value of 0.1 means to wait
        an extra 0%-10% (randomly determined) of the delay
        calculated using the previous three parameters.
    :param int reconnect_attempts_max: maximum attempts to reconnect
    :param bool use_ssl: deprecated, see :py:meth:`set_ssl`
    :param ssl_cert_file: deprecated, see :py:meth:`set_ssl`
    :param ssl_key_file: deprecated, see :py:meth:`set_ssl`
    :param ssl_ca_certs: deprecated, see :py:meth:`set_ssl`
    :param ssl_cert_validator: deprecated, see :py:meth:`set_ssl`
    :param ssl_version: deprecated, see :py:meth:`set_ssl`
    :param timeout: the timeout value to use when connecting the stomp socket
    :param bool wait_on_receipt: deprecated, ignored
    :param keepalive: some operating systems support sending the occasional heart
        beat packets to detect when a connection fails.  This
        parameter can either be set set to a boolean to turn on the
        default keepalive options for your OS, or as a tuple of
        values, which also enables keepalive packets, but specifies
        options specific to your OS implementation
    :param str vhost: specify a virtual hostname to provide in the 'host' header of the connection
    :param int recv_bytes: the number of bytes to use when calling recv
    """

    def __init__(self,
                 hosts_and_ports_and_paths=None,
                 prefer_localhost=True,
                 try_loopback_connect=True,
                 reconnect_sleep_initial=0.1,
                 reconnect_sleep_increase=0.5,
                 reconnect_sleep_jitter=0.1,
                 reconnect_sleep_max=60.0,
                 reconnect_attempts_max=3,
                 use_ssl=False,
                 ssl_key_file=None,
                 ssl_cert_file=None,
                 ssl_ca_certs=None,
                 ssl_cert_validator=None,
                 wait_on_receipt=False,
                 ssl_version=None,
                 timeout=None,
                 keepalive=None,
                 vhost=None,
                 auto_decode=True,
                 recv_bytes=1024
                 ):
        BaseTransport.__init__(self, wait_on_receipt, auto_decode)

        if hosts_and_ports_and_paths is None:
            hosts_and_ports_and_paths = [('localhost', 15674, 'ws')]

        sorted_hosts_and_ports = []
        sorted_hosts_and_ports.extend(hosts_and_ports_and_paths)

        #
        # If localhost is preferred, make sure all (host, port) tuples that refer to the local host come first in
        # the list
        #
        if prefer_localhost:
            sorted_hosts_and_ports.sort(key=utils.is_localhost)

        #
        # Assemble the final, possibly sorted list of (host, port) tuples
        #
        self.__hosts_and_ports = []
        self.__hosts_and_ports.extend(sorted_hosts_and_ports)

        self.__reconnect_sleep_initial = reconnect_sleep_initial
        self.__reconnect_sleep_increase = reconnect_sleep_increase
        self.__reconnect_sleep_jitter = reconnect_sleep_jitter
        self.__reconnect_sleep_max = reconnect_sleep_max
        self.__reconnect_attempts_max = reconnect_attempts_max
        self.__timeout = timeout

        self.socket = None
        self.__socket_semaphore = threading.BoundedSemaphore(1)
        self.current_host_and_port = None

        # setup SSL
        # TODO: Consider this somehow

        self.__keepalive = keepalive
        self.vhost = vhost
        self.__recv_bytes = recv_bytes

    def is_connected(self):
        """
        Return true if the socket managed by this connection is connected

        :rtype: bool
        """
        return self.socket is not None and self.socket.connected

    def disconnect_socket(self):
        """
        Disconnect the underlying socket connection
        """
        self.running = False

        if self.socket is not None:
            self.socket.close()

        self.current_host_and_port = None
        self.socket = None
        self.notify('disconnected')

    def send(self, encoded_frame):
        """
        :param bytes encoded_frame:
        """
        if self.socket is not None:
            try:
                with self.__socket_semaphore:
                    # TODO: Was 'sendall', check if this is correct
                    self.socket.send(encoded_frame)
            except Exception:
                _, e, _ = sys.exc_info()
                log.error("Error sending frame", exc_info=1)
                raise e
        else:
            raise exception.NotConnectedException()

    def receive(self):
        """
        :rtype: bytes
        """
        try:
            return self.socket.recv_data()[1]
        except Exception:
            traceback.print_exc()

    def cleanup(self):
        """
        Close the socket and clear the current host and port details.
        """
        try:
            self.socket.close()
        except:
            pass  # ignore errors when attempting to close socket
        self.socket = None
        self.current_host_and_port = None

    def attempt_connection(self):
        """
        Try connecting to the (host, port) tuples specified at construction time.
        """
        self.connection_error = False
        sleep_exp = 1
        connect_count = 0

        while self.running and self.socket is None and connect_count < self.__reconnect_attempts_max:
            for host_and_port in self.__hosts_and_ports:
                try:
                    log.info("Attempting connection to websocket %s", host_and_port)
                    self.socket = websocket.WebSocket()
                    ws_uri = 'ws://{}:{}/{}'.format(host_and_port[0], host_and_port[1], host_and_port[2])
                    print(ws_uri)
                    self.socket.connect(ws_uri,
                                        timeout=self.__timeout)
                    # TODO: Necessary?
                    #self.__enable_keepalive()
                    #need_ssl = self.__need_ssl(ws_uri)

                    #if self.blocking is not None:
                    #    self.socket.setblocking(self.blocking)

                    self.current_host_and_port = host_and_port
                    log.info("Established connection to host %s, port %s", host_and_port[0], host_and_port[1])
                    break
                except:
                    # Todo find explicit exception
                    self.socket = None
                    connect_count += 1
                    log.warning("Could not connect to host %s, port %s", host_and_port[0], host_and_port[1], exc_info=1)

            if self.socket is None:
                sleep_duration = (min(self.__reconnect_sleep_max,
                                      ((self.__reconnect_sleep_initial / (1.0 + self.__reconnect_sleep_increase))
                                       * math.pow(1.0 + self.__reconnect_sleep_increase, sleep_exp)))
                                  * (1.0 + random.random() * self.__reconnect_sleep_jitter))
                sleep_end = monotonic() + sleep_duration
                log.debug("Sleeping for %.1f seconds before attempting reconnect", sleep_duration)
                while self.running and monotonic() < sleep_end:
                    time.sleep(0.2)

                if sleep_duration < self.__reconnect_sleep_max:
                    sleep_exp += 1

        if not self.socket:
            raise exception.ConnectFailedException()


class WebsocketConnection(BaseConnection, Protocol11):
    """
    Represents a 1.2 connection (comprising transport plus 1.2 protocol class)
    See :py:class:`stomp.transport.Transport` for details on the initialisation parameters.
    """
    def __init__(self,
                 host_and_port_and_path=None,
                 prefer_localhost=False,
                 try_loopback_connect=True,
                 reconnect_sleep_initial=0.1,
                 reconnect_sleep_increase=0.5,
                 reconnect_sleep_jitter=0.1,
                 reconnect_sleep_max=60.0,
                 reconnect_attempts_max=3,
                 use_ssl=False,
                 ssl_key_file=None,
                 ssl_cert_file=None,
                 ssl_ca_certs=None,
                 ssl_cert_validator=None,
                 wait_on_receipt=False,
                 ssl_version=DEFAULT_SSL_VERSION,
                 timeout=None,
                 heartbeats=(0, 0),
                 keepalive=None,
                 vhost=None,
                 auto_decode=True,
                 auto_content_length=True,
                 heart_beat_receive_scale=1.5,
                 recv_byte=1024):
        transport = WebsocketTransport(host_and_port_and_path, prefer_localhost, try_loopback_connect,
                                       reconnect_sleep_initial, reconnect_sleep_increase, reconnect_sleep_jitter,
                                       reconnect_sleep_max, reconnect_attempts_max, use_ssl, ssl_key_file, ssl_cert_file,
                                       ssl_ca_certs, ssl_cert_validator, wait_on_receipt, ssl_version, timeout,
                                       keepalive, vhost, auto_decode)
        BaseConnection.__init__(self, transport)
        Protocol11.__init__(self, transport, heartbeats, auto_content_length, heart_beat_receive_scale=heart_beat_receive_scale)

    def connect(self, *args, **kwargs):
        self.transport.start()
        Protocol11.connect(self, *args, **kwargs)

    def disconnect(self, receipt=None, headers=None, **keyword_headers):
        """
        Call the protocol disconnection, and then stop the transport itself.

        :param str receipt: the receipt to use with the disconnect
        :param dict headers: a map of any additional headers to send with the disconnection
        :param keyword_headers: any additional headers to send with the disconnection
        """
        Protocol11.disconnect(self, receipt, headers, **keyword_headers)
        self.transport.stop()