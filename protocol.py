"""
protocol.py - Binary packet format for ResilienceFlow.

All packets share a 5-byte header:
  [1B version][1B type][2B payload_len][1B flags]

Packed as: struct.pack("!BBHB", version, pkt_type, payload_len, flags)

This replaces the verbose JSON packets from Workshop 4 with minimal
struct-packed binary headers for efficient transmission under congestion.
"""

import struct
import socket

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

VERSION = 0x01

HEADER_FMT = "!BBHB"
HEADER_SIZE = struct.calcsize(HEADER_FMT)  # 5 bytes

# Packet types
PKT_HEARTBEAT_REQ = 0x01
PKT_HEARTBEAT_ACK = 0x02
PKT_ASK           = 0x10
PKT_REPLY         = 0x11
PKT_MESSAGE       = 0x20
PKT_DATA          = 0x30   # Reliable UDP data frame
PKT_DATA_ACK      = 0x31   # Selective ACK
PKT_SOS           = 0x40
PKT_LOCATION      = 0x41
PKT_FILE_META     = 0x50   # File transfer announcement
PKT_FILE_CHUNK    = 0x51   # File chunk within reliable stream

# Header flags (bitfield)
FLAG_PRIORITY = 0x80
FLAG_SOS      = 0x40
FLAG_LOCATION = 0x20
FLAG_EOF      = 0x10

# Human-readable names for logging
PKT_NAMES = {
    PKT_HEARTBEAT_REQ: "HB_REQ",
    PKT_HEARTBEAT_ACK: "HB_ACK",
    PKT_ASK:           "ASK",
    PKT_REPLY:         "REPLY",
    PKT_MESSAGE:       "MESSAGE",
    PKT_DATA:          "DATA",
    PKT_DATA_ACK:      "DATA_ACK",
    PKT_SOS:           "SOS",
    PKT_LOCATION:      "LOCATION",
    PKT_FILE_META:     "FILE_META",
    PKT_FILE_CHUNK:    "FILE_CHUNK",
}


# ---------------------------------------------------------------------------
# Generic header pack / unpack
# ---------------------------------------------------------------------------

def pack_packet(pkt_type: int, payload: bytes, flags: int = 0) -> bytes:
    """Build a complete packet: 5-byte header + payload."""
    header = struct.pack(HEADER_FMT, VERSION, pkt_type, len(payload), flags)
    return header + payload


def unpack_header(data: bytes):
    """Parse the 5-byte header.
    Returns (pkt_type, payload_len, flags, payload_bytes).
    """
    if len(data) < HEADER_SIZE:
        raise ValueError(f"Packet too short: {len(data)} < {HEADER_SIZE}")
    version, pkt_type, payload_len, flags = struct.unpack(
        HEADER_FMT, data[:HEADER_SIZE]
    )
    if version != VERSION:
        raise ValueError(f"Unknown protocol version: 0x{version:02x}")
    payload = data[HEADER_SIZE : HEADER_SIZE + payload_len]
    return pkt_type, payload_len, flags, payload


# ---------------------------------------------------------------------------
# Heartbeat  (8 bytes payload = uint64 monotonic_ns timestamp)
# ---------------------------------------------------------------------------

def pack_heartbeat_req(ts_ns: int) -> bytes:
    return pack_packet(PKT_HEARTBEAT_REQ, struct.pack("!Q", ts_ns))


def pack_heartbeat_ack(ts_ns: int) -> bytes:
    return pack_packet(PKT_HEARTBEAT_ACK, struct.pack("!Q", ts_ns))


def unpack_heartbeat(payload: bytes) -> int:
    """Returns the echoed / original timestamp in nanoseconds."""
    (ts_ns,) = struct.unpack("!Q", payload[:8])
    return ts_ns


# ---------------------------------------------------------------------------
# Discovery  (ASK = 4 bytes sender IPv4 ;  REPLY = 4 bytes IP + UTF-8 name)
# ---------------------------------------------------------------------------

def pack_ask(ip_str: str) -> bytes:
    return pack_packet(PKT_ASK, socket.inet_aton(ip_str))


def unpack_ask(payload: bytes) -> str:
    return socket.inet_ntoa(payload[:4])


def pack_reply(ip_str: str, name: str) -> bytes:
    payload = socket.inet_aton(ip_str) + name.encode("utf-8")
    return pack_packet(PKT_REPLY, payload)


def unpack_reply(payload: bytes):
    """Returns (ip_str, name)."""
    ip_str = socket.inet_ntoa(payload[:4])
    name = payload[4:].decode("utf-8")
    return ip_str, name


# ---------------------------------------------------------------------------
# Message  (UTF-8 text)
# ---------------------------------------------------------------------------

def pack_message(text: str) -> bytes:
    return pack_packet(PKT_MESSAGE, text.encode("utf-8"))


def unpack_message(payload: bytes) -> str:
    return payload.decode("utf-8")


# ---------------------------------------------------------------------------
# Reliable DATA frame  [4B seq_uint32][chunk bytes ...]
# ---------------------------------------------------------------------------

def pack_data(seq: int, chunk: bytes, flags: int = 0) -> bytes:
    payload = struct.pack("!I", seq) + chunk
    return pack_packet(PKT_DATA, payload, flags)


def unpack_data(payload: bytes):
    """Returns (seq, chunk_bytes)."""
    (seq,) = struct.unpack("!I", payload[:4])
    return seq, payload[4:]


# ---------------------------------------------------------------------------
# Selective ACK  [4B acked_seq][2B rwnd][2B nack_count][4B * nack_seqs]
# ---------------------------------------------------------------------------

def pack_data_ack(acked_seq: int, rwnd: int, nacks: list = None) -> bytes:
    if nacks is None:
        nacks = []
    payload = struct.pack("!IHH", acked_seq, rwnd, len(nacks))
    for ns in nacks:
        payload += struct.pack("!I", ns)
    return pack_packet(PKT_DATA_ACK, payload)


def unpack_data_ack(payload: bytes):
    """Returns (acked_seq, rwnd, [nack_seqs])."""
    acked_seq, rwnd, nack_count = struct.unpack("!IHH", payload[:8])
    nacks = []
    off = 8
    for _ in range(nack_count):
        (ns,) = struct.unpack("!I", payload[off : off + 4])
        nacks.append(ns)
        off += 4
    return acked_seq, rwnd, nacks


# ---------------------------------------------------------------------------
# SOS  (UTF-8 text, FLAG_SOS | FLAG_PRIORITY set)
# ---------------------------------------------------------------------------

def pack_sos(text: str) -> bytes:
    return pack_packet(PKT_SOS, text.encode("utf-8"), FLAG_SOS | FLAG_PRIORITY)


def unpack_sos(payload: bytes) -> str:
    return payload.decode("utf-8")


# ---------------------------------------------------------------------------
# Location  (2x float32 = 8 bytes: latitude, longitude)
# ---------------------------------------------------------------------------

def pack_location(lat: float, lon: float) -> bytes:
    payload = struct.pack("!ff", lat, lon)
    return pack_packet(PKT_LOCATION, payload, FLAG_LOCATION | FLAG_PRIORITY)


def unpack_location(payload: bytes):
    """Returns (lat, lon)."""
    lat, lon = struct.unpack("!ff", payload[:8])
    return lat, lon


# ---------------------------------------------------------------------------
# File Meta  [4B total_chunks][4B file_size][UTF-8 filename]
# ---------------------------------------------------------------------------

def pack_file_meta(filename: str, total_chunks: int, file_size: int) -> bytes:
    payload = struct.pack("!II", total_chunks, file_size) + filename.encode("utf-8")
    return pack_packet(PKT_FILE_META, payload)


def unpack_file_meta(payload: bytes):
    """Returns (filename, total_chunks, file_size)."""
    total_chunks, file_size = struct.unpack("!II", payload[:8])
    filename = payload[8:].decode("utf-8")
    return filename, total_chunks, file_size


# ---------------------------------------------------------------------------
# File Chunk  [4B chunk_seq][raw bytes]   (FLAG_EOF set on last chunk)
# ---------------------------------------------------------------------------

def pack_file_chunk(chunk_seq: int, data: bytes, is_eof: bool = False) -> bytes:
    flags = FLAG_EOF if is_eof else 0
    payload = struct.pack("!I", chunk_seq) + data
    return pack_packet(PKT_FILE_CHUNK, payload, flags)


def unpack_file_chunk(payload: bytes):
    """Returns (chunk_seq, raw_bytes)."""
    (chunk_seq,) = struct.unpack("!I", payload[:4])
    return chunk_seq, payload[4:]
