# -*- coding: utf-8 -*-
"""
:copyright: NSN
:author: Rafal Jasicki
:contact: rafal.jasicki@nsn.com
"""
import socket
import zlib
import threading
from Queue import Queue
from udpcpmessage import UdpcpMessage, CorruptedMessage
import logging
import time




class UdpcpSyncFailed(Exception):
    pass


class UdpcpConnectionInternal(object):
    def __init__(self, target, local=('127.0.0.1', 13001), timeout=0.05,
                 ack_delay=2.0, max_retries=8, max_payload_size=2048, no_sync=False):
        self.no_sync = no_sync
        self.target = target
        self.local = local
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        if self.local and self.local[0]:
            self.socket.bind(self.local)
        self.socket.settimeout(timeout)
        self.alive = False
        self.received = Queue()
        self.send_queue = Queue()
        self.status_queue = Queue()
        self.waiting_for_ack = {}
        self.last_id = None
        self.noAck = False
        self.singleAck = True
        self.ack_delay = ack_delay
        self.max_retries = max_retries
        self.message_history = {}
        self.max_payload_size = max_payload_size
        self.message_parts = {}
        self.logger = logging.getLogger("dev")
        self.name = None

    def update_msg(self, msg, message_id=None, part=0, count=1):
        """
        Update message with correct ID (before sending).
        """

        msg.header.noAck = self.noAck
        msg.header.singleAck = self.singleAck
        if message_id is None:
            self.last_id = (self.last_id + 1) % 0xffff
            if self.last_id == 0:
                self.logger.debug("Roll-over of messageId.")
                self.last_id += 1
                self.message_history = {}
            msg.header.messageId = self.last_id
        else:
            msg.header.messageId = message_id

        msg.header.fragmentAmount = count
        msg.header.fragmentNumber = part
        msg.update_checksum()

    def _register_message(self, msg):
        """
        If message needs to be acked by other side it should be registered here.
        """
        if msg.header.noAck:
            self.status_queue.put(('Message sent', 'no ack', msg.header.messageId))
            return
        self.logger.info("Message awaiting ack added "
                                      "(id: {}, part: {}).".format(msg.header.messageId,
                                                                   msg.header.fragmentNumber))
        self.waiting_for_ack[(msg.header.messageId,
                              msg.header.fragmentNumber)] = [msg, time.time() +
                                                             self.ack_delay, 0]

    def _check_retries(self):
        """
        Run periodicly to check if unacked message does not need to be resent.
        If message has been resend maximum number of times it will be discarded and info
        will be sent to status_queue.
        """
        t = time.time()
        for (msg_id, fragmentNumber), item in self.waiting_for_ack.items():
            if item[1] > t:
                continue
            if item[2] < self.max_retries:
                self._send(item[0])
                item[2] += 1
                item[1] = t + self.ack_delay
                continue
            self.logger.info("Message discarded due to exceeded number of retries.")
            self.status_queue.put(('Message failed', 'ack', msg_id))
            del self.waiting_for_ack[(msg_id, fragmentNumber)]

    def send_sync_message(self):
        """
        Send synchronisation message at start of UDPCP communication.
        """
        if (0, 0) not in self.waiting_for_ack:
            sync_msg = UdpcpMessage()
            self.message_history = {}
            self._send(sync_msg)
            self._register_message(sync_msg)

    def sync(self):
        """
        Check if sync with RF/LMTS is needed and if it is perform it.
        """
        if self.last_id is not None:
            return
        if self.no_sync and self.last_id is None:
            self.logger.info("IPHY hack -- no sync.")
            self.last_id = 0
            return
        self.logger.info("UDPCP Sync started.")
        while self.last_id is None:
            self.send_sync_message()
            self._receive()
            self._check_retries()
            if (0, 0) not in self.waiting_for_ack and self.last_id is None:
                self.logger.error("UDPCP Sync Failed!")
                raise UdpcpSyncFailed("UDPCP Sync Failed!")
        self.logger.info("UDPCP Sync finished.")

    def _send(self, msg):
        """
        Send message over vetwork.
        """
        self.logger.debug("Message sent over network.")
        self.logger.debug("\n---{}--->\n{}".format(self.name, msg))
        # self.logger.debug("\nBYTES:\n{}".format(msg.to_bytes()))
        self.logger.debug("\nTARGET:\n{}".format(self.target))

        self.socket.sendto(msg.to_bytes(), self.target)

    def _send_from_queue(self):
        """
        Take message from queue and prepare it for sending.
        """
        msgs = self.send_queue.get()
        inx = 0
        count = len(msgs)
        for m in msgs:
            if inx == 0:
                self.update_msg(m, part=inx, count=count)
                m_id = m.header.messageId
            else:
                self.update_msg(m, message_id=m_id, part=inx, count=count)
            self.logger.debug("Message {m.header.messageId}, {m.header.fragmentNumber} ready for sending.".format(m=m))
            if count > 1:
                self.logger.debug("This is multipart ({}/{}) message".format(inx+1, count))
            self._send(m)
            self._register_message(m)
            inx += 1
        return m_id

    def _ack(self, msg, duplicate=False):
        """
        Check if received message needs acking and ack it.
        """
        # return if either ack not required or message is of ack type
        self.logger.debug("Ack check for id:{}.".format(msg.header.messageId))
        if msg.header.noAck or msg.header.messageType == 0b10:
            return
        ack_msg = msg.create_ack(duplicate)
        self.logger.info("Ack for message (id: {}) created.".format(ack_msg.header.messageId))
        self._send(ack_msg)

    def _receive(self):
        """
        Receive data from socket.
        """
        try:
            data = self.socket.recv(4096)
        except socket.timeout as e:
            self.logger.debug("No data.")
            return False
        except socket.error as e:
            self.logger.warning('Error on receive: ' + str(e))
            return False

        if len(data) < 12:
            self.logger.warn("Data to short for valid message.")
            return False

        return self._handle_received_data(data)

    def _handle_received_data(self, data):
        """
        Try creating message from data and handle it.
        """
        try:
            m = UdpcpMessage(data)
        except CorruptedMessage as e:
            self.logger.warn("Corrupted data: {}".format(e))
            return True
        self.logger.debug("\n<---{}---\n{}".format(self.name, m))
        return self._handle_received_message(m)

    def _handle_received_message(self, msg):
        """
        Message handling.
        """
        if msg.header.messageType == 0b1:
            return self._handle_data_message(msg)
        # this is an Ack!
        return self._handle_received_ack(msg)

    def _msg_ack_received(self, msg):
        """
        After receiving ack remove it from dictionary of messages that require acking.
        """
        if (msg.header.messageId, msg.header.fragmentNumber) not in self.waiting_for_ack:
            return
        m = self.waiting_for_ack[(msg.header.messageId, msg.header.fragmentNumber)][0]
        if not m.header.singleAck:
            self._handle_ack_multi(msg)
            return
        self._handle_ack_single(msg)

    def _handle_ack_single(self, msg):
        """Ack for single-ack messages"""
        for n in xrange(msg.header.fragmentAmount):
            if (msg.header.messageId, n) in self.waiting_for_ack:
                del self.waiting_for_ack[(msg.header.messageId, n)]
        self.status_queue.put(('Message sent', 'ack', msg.header.messageId))
        self.logger.debug("Message id:{} acked (single ack).".format(msg.header.messageId))

    def _handle_ack_multi(self, msg):
        """Ack for multi-ack messages"""
        del self.waiting_for_ack[(msg.header.messageId, msg.header.fragmentNumber)]
        for n in xrange(msg.header.fragmentAmount):
            if (msg.header.messageId, n) in self.waiting_for_ack:
                return
        self.status_queue.put(('Message sent', 'ack', msg.header.messageId))
        self.logger.debug("Message id:{} acked (multiple ack).".format(msg.header.messageId))

    def _handle_data_message(self, msg):
        """
        Handle received data message.
        """
        m_id = msg.header.messageId
        handled = False
        if m_id in self.message_history:
            self.logger.info("Duplicate received.")
            self._ack(msg)
            return True

        if m_id not in self.message_parts:
            self.logger.info("New {}-part message.".format(msg.header.fragmentAmount))
            self.message_parts[m_id] = [None for _ in
                                        range(msg.header.fragmentAmount)]
        if self.message_parts[m_id][msg.header.fragmentNumber] is None:
            self.logger.info("New part {} of {} "
                                          "received.".format(msg.header.fragmentNumber+1,
                                                             msg.header.fragmentAmount))
            self.message_parts[m_id][msg.header.fragmentNumber] = msg
            handled = True
            if not msg.header.singleAck:
                self._ack(msg)
        else:
            self.logger.info("Duplicate received.")
            self._ack(msg)
            return True
        return self._message_complete_check(msg) or handled

    def _message_complete_check(self, msg):
        """
        Check if multipart message is complete.
        """
        m_id = msg.header.messageId
        if None not in self.message_parts[m_id]:
            self.logger.info("Multipart message with id:{} complete.".format(m_id))
            self.received.put(self.message_parts[m_id])
            self.message_history[m_id] = self.message_parts[m_id]
            if msg.header.singleAck:
                self._ack(self.message_history[m_id][0])
            del self.message_parts[m_id]
            return True
        return False

    def _handle_received_ack(self, msg):
        """
        Handle received ack-message.
        """
        if msg.header.messageId == 0 and self.last_id is None:
            self.logger.info("Ack on sync request received.")
            self.last_id = 0
        if (msg.header.messageId, msg.header.fragmentNumber) in self.waiting_for_ack:
            self.logger.debug("Message id:{} ack received...".format(msg.header.messageId))
            self._msg_ack_received(msg)
        return True

    def listen(self):
        """
        Start listening for incoming messages.
        """
        self.logger.info("Starting listening on {}.".format(self.local))
        self.alive = True
        while self.alive:
            while self._receive():
                continue
            while not self.send_queue.empty():
                self.sync()
                self._send_from_queue()
            self._check_retries()
        self.socket.close()
        self.socket = None


class UdpcpConnection(UdpcpConnectionInternal):
    def send(self, msg):
        """
        Send message (adds to sending queue).
        """
        self.send_queue.put([msg])

    def start_listener(self):
        """
        Start internal thread for handling messages.
        """
        self.thread = threading.Thread(target=self.listen)
        self.thread.start()

    def stop_listener(self):
        """
        Stop internal handling of messages.
        """
        self.logger.info("Listening stopped.")
        self.alive = False

    def create_multipart_message(self, payload):
        """
        Create one- or multi- part message from payload.
        """
        msgs = []
        inx = 0
        while inx < len(payload):
            m = (UdpcpMessage(payload=payload[inx:inx+self.max_payload_size]))
            m.header.dataLength = len(payload)
            m.update_checksum()
            msgs.append(m)
            inx += self.max_payload_size
        return msgs

    def send_multipart_message(self, msg_list):
        """
        Add multipart message to sending queue.
        """
        self.send_queue.put(msg_list)


def main():
    """
    Quick testing.
    """
    conn = UdpcpConnection(target=("127.0.0.1", 10000), max_payload_size=1400)
    conn.start_listener()
    time.sleep(1)

    # message in part
    conn.singleAck = False
    payload_string = """<SOAP-ENV:Envelope />""" * 20

    messages_to_send = conn.create_multipart_message(payload=payload_string)
    conn.send_multipart_message(msg_list=messages_to_send)
    time.sleep(5)
    conn.stop_listener()


if __name__ == '__main__':
    main()
