# -*- coding: utf-8 -*-
"""
:copyright: NSN
:author: Rafal Jasicki
:contact: rafal.jasicki@nsn.com
"""
import udpcp
import pytest
import time
import re
import copy


def create_connections(no_sync=False):
    # self.listener = SoapConnection(("127.0.0.1", 13002+port_inc), ('127.0.0.1', 13001+port_inc))
    # self.sender = SoapConnection(("127.0.0.1", 13001+port_inc), ('127.0.0.1', 13002+port_inc))
    listener = udpcp.UdpcpConnection((None, None), (None, None))
    listener.socket.sendto('aaa', ('127.0.0.1', 13010))
    l = ('127.0.0.1', listener.socket.getsockname()[1])
    sender = udpcp.UdpcpConnection(l, None, no_sync=no_sync)
    sender.socket.sendto('aaa', ('127.0.0.1', 13010))
    s = ('127.0.0.1', sender.socket.getsockname()[1])
    sender.target = l
    listener.target = s
    listener.name = 'List'
    sender.name = 'Send'
    listener.iphy_dirty_hack = False
    sender.iphy_dirty_hack = False
    listener._receive()
    sender._receive()

    return listener, sender


def test_create_multipart_message():
    sender = udpcp.UdpcpConnection(("127.0.0.1", 13001), ('127.0.0.1', 13002))
    payload = '0123456789'
    msgs = '0123456789' * 3 + 'abcde'
    sender.max_payload_size = 10
    msg_list = sender.create_multipart_message(msgs)
    assert len(msg_list) == 4
    assert msg_list[2].payload == payload
    assert msg_list[3].payload == 'abcde'


def test_send_multipart_message():
    listener, sender = create_connections()
    sender.send_sync_message()
    listener._receive()
    assert not listener.received.empty()
    listener.received.get()
    sender._receive()

    msgs = '0123456789' * 3 + 'abcde'
    sender.max_payload_size = 10

    msg_list = sender.create_multipart_message(msgs)
    sender.send_multipart_message(msg_list)
    sender._send_from_queue()
    assert sender.send_queue.empty()
    # to remove sync message from queue

    m_id = sender.last_id
    for i in xrange(4):
        print i
        assert listener.received.empty()
        listener._receive()
    assert not listener.received.empty()
    assert str(msg_list) == str(listener.received.get())
    # ack received on sender
    assert sender._receive()
    assert sender.waiting_for_ack == {}
    # assert False


def test_long_message_with_payload():
    txt = \
        "5300C36F520001000001028F3C534F41502D454E563A456E76656C6F706520786D6C6E7" \
        "33A534F41502D454E563D22687474703A2F2F736368656D61732E786D6C736F61702E6F" \
        "72672F736F61702F656E76656C6F70652F223E090D0A093C534F41502D454E563A48656" \
        "16465723E0D0A09093C746F3E4652312F4C54583C2F746F3E0D0A09093C66726F6D3E42" \
        "54535F4F4D3C2F66726F6D3E0D0A09093C69643E313030313C2F69643E0D0A09093C616" \
        "374696F6E3E4F425341495F434D3C2F616374696F6E3E0D0A09093C76657273696F6E3E" \
        "322E303C2F76657273696F6E3E0D0A093C2F534F41502D454E563A4865616465723E0D0" \
        "A093C534F41502D454E563A426F64793E0D0A09093C6D6F64756C655265616479496E64" \
        "3E0D0A0909093C6D6F64756C65547970653E4C54583C2F6D6F64756C65547970653E0D0" \
        "A0909093C76656E646F723E4E6F6B69613C2F76656E646F723E0D0A0909093C70726F64" \
        "756374436F64653E343730313030412E3130323C2F70726F64756374436F64653E0D0A0" \
        "909093C73657269616C4E756D3E4C313033313531323334353C2F73657269616C4E756D" \
        "3E0D0A0909093C7375625261636B3E31313C2F7375625261636B3E0D0A0909093C736C6" \
        "F743E313C2F736C6F743E0D0A0909093C626C6456657273696F6E3E302E32303C2F626C" \
        "6456657273696F6E3E0D0A0909093C7265737461727443617573653E706F7765725F6F6" \
        "E3C2F7265737461727443617573653E0D0A0909093C74696D657256616C75653E36303C" \
        "2F74696D657256616C75653E0D0A0909093C6877537749643E323C2F6877537749643E0" \
        "D0A09093C2F6D6F64756C655265616479496E643E0D0A093C2F534F41502D454E563A42" \
        "6F64793E0D0A3C2F534F41502D454E563A456E76656C6F70653E0D0A"
    data = str(bytearray(map(lambda x: int(x, 16), re.findall("..", txt))))
    m = udpcp.UdpcpMessage(data=data)

    m2 = copy.deepcopy(m)
    m2.payload = '123'
    m2.update_checksum()
    # dataLength is checked for whole multipart message!
    # with pytest.raises(udpcp.CorruptedMessage):
    #     m3 = udpcp.UdpcpMessage(data=str(m2.to_bytes()))


def test_message_decoding():
    raw = [2, 170, 0, 85, 83, 0, 1, 0, 0, 0, 0, 0]  # this is sync message
    m = udpcp.UdpcpMessage()
    m.to_bytes()
    m_raw = udpcp.UdpcpMessage(str(bytearray(raw)))
    m_raw.to_bytes()
    s1 = str(m)
    s2 = str(m_raw)
    assert s1 == s2


def test_message_with_payload():
    with pytest.raises(Exception):
        # cannot create message with both raw data and pyload
        m = udpcp.UdpcpMessage(data='xxxxxxxxxxxxx', payload='<xml></xml>')

    p = '<xml></xml>'
    m = udpcp.UdpcpMessage(payload=p)
    assert m.payload == p
    cs = m.header.checksum
    m.update_checksum()
    assert cs == m.header.checksum


def test_sync_message():
    m = udpcp.UdpcpMessage()
    listener, sender = create_connections()
    sender._send(m)
    sender._register_message(m)
    listener._receive()
    assert not listener.received.empty()
    m2 = listener.received.get()[0]
    assert str(m) == str(m2)
    # check if ack received
    sender._receive()
    assert sender.last_id == 0


def test_auto_sync():
    listener, sender = create_connections()
    # run sync
    sender.send_sync_message()
    listener._receive()
    sender._receive()
    # verify sync
    assert sender.last_id == 0
    assert not listener.received.empty()


def test_ack():
    m = udpcp.UdpcpMessage()
    listener, sender = create_connections()
    sender.send(m)
    # run sync
    sender.send_sync_message()
    listener._receive()
    sender._receive()
    # verify sync
    assert sender.last_id == 0
    # send the message
    sender._send_from_queue()
    assert (m.header.messageId, 0) in sender.waiting_for_ack.keys()
    # check that ack is required
    assert not listener.received.empty()
    listener.received.get()
    assert listener.received.empty()
    listener._receive()

    assert not listener.received.empty()
    m2 = listener.received.get()[0]
    assert str(m) == str(m2)

    # check if ack received
    assert sender.received.empty()
    assert sender._receive()
    assert sender.received.empty()
    # check if ack was accepted
    assert (m.header.messageId, 0) not in sender.waiting_for_ack.keys()


def test_resend():
    """
    Tests if message that has not been acked is resend for the second
    time.
    """
    listener, sender = create_connections()
    # run sync
    sender.ack_delay = 0.1
    sender.max_retries = 4
    sender.send_sync_message()
    assert (0, 0) in sender.waiting_for_ack.keys()
    time.sleep(0.12)
    sender._check_retries()
    # first received copy
    listener._receive()
    assert not listener.received.empty()
    m1 = listener.received.get()[0]
    # second received copy ...
    listener._receive()
    # .. is silently discarded
    assert listener.received.empty()
    sender._receive()
    assert sender.waiting_for_ack == {}
    sender._receive()
    assert sender.received.empty()
    # [TODO] if possible test if second ack is marked as duplicate


def test_resend_failed():
    """Tests if after maximum number of retries message is discarded."""
    listener, sender = create_connections()
    # run sync
    sender.ack_delay = 0.01
    sender.max_retries = 4
    sender.send_sync_message()
    assert (0, 0) in sender.waiting_for_ack.keys()
    for i in xrange(sender.max_retries):
        time.sleep(0.2)
        sender._check_retries()
        assert sender.waiting_for_ack[(0, 0)][2] == i + 1
    sender._receive()
    assert (0, 0) in sender.waiting_for_ack.keys()
    sender._check_retries()
    assert sender.waiting_for_ack == {}


def test_sending_three_part_message_with_payload():
    listener, sender = create_connections()
    listener.max_payload_size = 4
    sender.max_payload_size = 4
    # run sync
    sender.send_sync_message()
    listener._receive()
    assert not listener.received.empty()
    listener.received.get()
    sender._receive()
    # verify sync
    assert sender.last_id == 0
    # create paylod string with value to send and add it to the queue
    payload_string = "111122223333"
    messages_to_send = sender.create_multipart_message(payload=payload_string)
    sender.send_multipart_message(msg_list=messages_to_send)
    # send the message
    sender._send_from_queue()
    # check if they are 3 (parts) messages watinig for ack
    assert len(sender.waiting_for_ack) == 3
    # receive all parts of message
    for i in xrange(3):
        # check is there any message in Queue
        assert listener.received.empty()
        listener._receive()
    # check empty Queue
    assert not listener.received.empty()
    # check received value
    assert str(messages_to_send) == str(listener.received.get())
    # check if ack received
    assert sender.received.empty()
    sender._receive()
    # check if there is any message watinig for ack
    assert sender.waiting_for_ack == {}


def test_sending_three_part_message_all_parts_ack_with_payload():
    listener, sender = create_connections()
    listener.max_payload_size = 4
    sender.max_payload_size = 4
    # set to False due to all single part should be ACK
    sender.singleAck = False
    # run sync
    sender.send_sync_message()
    listener._receive()
    assert not listener.received.empty()
    listener.received.get()
    sender._receive()
    # verify sync
    assert sender.last_id == 0
    # create paylod string with value to send and add it to the queue
    payload_string = "111122223333"
    messages_to_send = sender.create_multipart_message(payload=payload_string)
    sender.send_multipart_message(msg_list=messages_to_send)
    # send the message
    sender._send_from_queue()
    # check if they are 3 (parts) messages watinig for ack
    assert len(sender.waiting_for_ack) == 3
    print sender.waiting_for_ack
    # 1st message
    assert listener.received.empty()
    listener._receive()
    assert len(sender.waiting_for_ack) == 3
    sender._receive()
    assert len(sender.waiting_for_ack) == 2
    # 2nd message
    assert listener.received.empty()
    listener._receive()
    assert len(sender.waiting_for_ack) == 2
    sender._receive()
    assert len(sender.waiting_for_ack) == 1
    # 3rd message
    assert listener.received.empty()
    listener._receive()
    # check if all parts are recived
    assert str(messages_to_send) == str(listener.received.get())
    assert len(sender.waiting_for_ack) == 1
    sender._receive()
    # check if all parts are ACK
    assert sender.waiting_for_ack == {}
