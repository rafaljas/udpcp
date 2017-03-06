# -*- coding: utf-8 -*-
"""
:copyright: NSN
:author: Rafal Jasicki
:contact: rafal.jasicki@nsn.com
"""
import zlib
import logging

logger = logging.getLogger('dev')


class CorruptedMessage(ValueError):
    pass


class UdpcpMessageHeader(object):
    """
    """
    checksum = 0x02aa0055   # 4B       - it will be updated in init so no worry
    messageType = 0b01      # 2b # 5B  - 01 for data, 10 for ack
    version = 0b010         # 3b
    noAck = False           # 1b       - no ack on any message
    useChecksum = True      # 1b       - if 0 'checksum' must be 0
    singleAck = True        # 1b       - ack only on last part
    duplicate = False       # 1b # 6B  - 0
    reserved = 0b0000000    # 7b       - must be 0..
    fragmentAmount = 0x01   # 1B # 7B  - 0 or 1 for not fragmented messages
    fragmentNumber = 0x00   # 1B # 8B  - 0 for first element
    messageId = 0x0000      # 2B # 10B - unique, used to complete fragmented messages,
    #                                    start with 0 for sync message
    dataLength = 0x0000     # 2B # 12B - length of data in octets

    def __init__(self, raw_data=None):
        """
        Creation of header from either binary data (raw_data) or with default
        values.
        """
        if raw_data is None:
            return
        data = [int(x.encode('hex'), 16) for x in raw_data[:12]]
        self.checksum = (data[0] << 24) + (data[1] << 16) + (data[2] << 8) + data[3]
        self.messageType = data[4] >> 6
        self.version = (data[4] >> 3) % 0b1000
        self.noAck = bool((data[4] >> 2) % 2)
        self.useChecksum = bool((data[4] >> 1) % 2)
        self.singleAck = bool(data[4] % 2)
        self.duplicate = bool((data[5] >> 7) % 2)
        self.fragmentAmount = data[6]
        self.fragmentNumber = data[7]
        self.messageId = (data[8] << 8) + data[9]
        self.dataLength = (data[10] << 8) + data[11]

    def calculate_checksum_for_data(self, data):
        """
        Calculate checksum for data.
        """
        cs = zlib.adler32(buffer(data))
        if cs % 0xffffffff != cs:
            cs = (cs % 0xffffffff) + 1
        return cs

    def to_bytes(self):
        """
        Create byte array from header.
        """
        tmp = []
        tmp += [self.checksum >> 24, (self.checksum >> 16) & 0xff, (self.checksum >> 8) & 0xff, self.checksum & 0xff]
        tmp.append((self.messageType << 6) + (self.version << 3) +
                   (int(self.noAck) << 2) + (int(self.useChecksum) << 1) +
                   int(self.singleAck))
        tmp.append((int(self.duplicate) << 7) + self.reserved)
        tmp.append(self.fragmentAmount)
        tmp.append(self.fragmentNumber)
        tmp += [self.messageId >> 8, self.messageId & 0xff]
        tmp += [self.dataLength >> 8, self.dataLength & 0xff]

        return bytearray(tmp)

    def __repr__(self):
        """
        __repr__ for message header
        """
        return ("UDPCP message header:\n"
                "\tchecksum: {m.checksum:#x}\n"
                "\tmessageType: {m.messageType:#b}\n"
                "\tversion: {m.version:#b}\n"
                "\tnoAck: {m.noAck}\n"
                "\tuseChecksum: {m.useChecksum}\n"
                "\tsingleAck: {m.singleAck}\n"
                "\tduplicate: {m.duplicate}\n"
                "\tfragmentAmount: {m.fragmentAmount:#x}\n"
                "\tfragmentNumber: {m.fragmentNumber:#x}\n"
                "\tmessageId: {m.messageId:#x}\n"
                "\tdataLength: {m.dataLength:#x}").format(m=self)


class UdpcpMessage(object):
    payload = None

    def __init__(self, data=None, payload=None, validate=True):
        """
        To create message from data received from socket -- use 'data'.
        To create new message with specified payload -- use 'payload'.
        If 'validate' is False checksum will not be checked when creating message
        from socket data.
        """
        if payload is not None and data is not None:
            raise Exception("UDPCP message should not be created with both "
                            "binnary data and payload.")
        self.header = UdpcpMessageHeader(data)
        if data is not None:
            self.init_from_data(data, validate)

        if payload is not None:
            self.payload = payload
            self.header.dataLength = len(payload)
            self.update_checksum()

    def init_from_data(self, data, validate):
        """
        Initialize payload from binary data. Check if message is not corrupted.
        """
        if len(data) > 12:
            self.payload = str(data[12:])

        cs = self.header.checksum
        self.update_checksum()
        if validate and self.header.useChecksum and abs(cs - self.header.checksum) > 1:
            logger.error('Corrupted UDPCP message received')
            raise CorruptedMessage("Checksum error.")

    @classmethod
    def calculate_checksum_for_data(cls, data):
        """
        Calculate checksum for provided data.
        """
        cs = zlib.adler32(buffer(data))
        if cs % 0xffffffff != cs:
            cs = (cs % 0xffffffff) + 1
        return cs

    def to_bytes(self):
        """
        Create byte representation of the message.
        """
        if self.payload:
            return self.header.to_bytes() + bytearray(self.payload)
        return self.header.to_bytes()

    def set_checksum(self, value):
        """
        Set checksum to defined value.
        """
        self.header.checksum = value

    def update_checksum(self):
        """
        Recalculate checksum for message.
        """
        self.header.checksum = 0
        if not self.header.useChecksum:
            return
        self.header.checksum = self.calculate_checksum_for_data(self.to_bytes())

    def create_ack(self, duplicate):
        """
        Create new UDPCP ack-message (response to this message).
        """
        m = UdpcpMessage()
        m.header.messageType = 0b10
        m.header.noAck = True
        m.header.singleAck = True
        m.header.messageId = self.header.messageId
        m.header.duplicate = duplicate
        m.header.fragmentNumber = self.header.fragmentNumber
        m.header.fragmentAmount = self.header.fragmentAmount
        m.update_checksum()
        return m

    def __repr__(self):
        """Print for UDPCP Message"""
        return '\nPayload:\n\t'.join([self.header.__repr__(), self.payload.__repr__()])
