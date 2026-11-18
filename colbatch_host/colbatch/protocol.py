"""Wire protocol between the colbatch host and its worker processes.

Transport: one pipe per direction (worker stdin = host->worker, worker
stdout = worker->host).  Frames are length-prefixed and self-describing so
the host can reject garbage (bad magic, absurd declared length) before
allocating anything.

Frame layout (all integers little-endian):

    magic    : 4 bytes   b"CBH1"
    mtype    : 1 byte    message type (see below)
    flags    : 1 byte    reserved, must be 0
    reserved : 2 bytes   reserved, must be 0
    length   : 8 bytes   unsigned, payload length in bytes

Payload:

    json_len : 4 bytes   unsigned, length of the JSON header
    json     : json_len bytes, UTF-8 JSON object (message metadata)
    blob     : remaining bytes (Arrow IPC stream for TASK/RESULT, else empty)

Message types:

    HELLO       worker->host   protocol version, plugin identity, schemas
    HELLO_ACK   host->worker   accept/reject + limits
    READY       worker->host   worker is ready to accept tasks
    TASK        host->worker   one RecordBatch to transform
    RESULT      worker->host   transformed RecordBatch
    BATCH_ERROR worker->host   plugin raised / contract self-check failed
    SHUTDOWN    host->worker   polite stop request
    BYE         worker->host   acknowledgement, worker exits right after

Every TASK/RESULT/BATCH_ERROR carries (batch_id, generation, attempt) so a
frame from an old process generation or a previous attempt can be identified
and discarded instead of being spliced onto the wrong batch.
"""

from __future__ import annotations

import json
import struct

MAGIC = b"CBH1"
PROTOCOL_VERSION = 1

HEADER = struct.Struct("<4sBBHQ")   # magic, mtype, flags, reserved, length
JSONLEN = struct.Struct("<I")
HEADER_SIZE = HEADER.size

HELLO = 1
HELLO_ACK = 2
READY = 3
TASK = 4
RESULT = 5
BATCH_ERROR = 6
SHUTDOWN = 7
BYE = 8

NAMES = {
    HELLO: "HELLO",
    HELLO_ACK: "HELLO_ACK",
    READY: "READY",
    TASK: "TASK",
    RESULT: "RESULT",
    BATCH_ERROR: "BATCH_ERROR",
    SHUTDOWN: "SHUTDOWN",
    BYE: "BYE",
}


class ProtocolError(Exception):
    """Raised when a peer violates framing rules."""


def encode(mtype: int, meta: dict, blob: bytes = b"") -> bytes:
    body = json.dumps(meta, ensure_ascii=False).encode("utf-8")
    payload = JSONLEN.pack(len(body)) + body + blob
    return HEADER.pack(MAGIC, mtype, 0, 0, len(payload)) + payload


def parse_header(buf) -> tuple[int, int]:
    """Parse a 16-byte header.  Returns (mtype, payload_length).

    Raises ProtocolError on bad magic.  The caller is responsible for
    checking payload_length against its configured maximum *before*
    waiting for / allocating the payload.
    """
    magic, mtype, flags, _reserved, length = HEADER.unpack(bytes(buf[:HEADER_SIZE]))
    if magic != MAGIC:
        raise ProtocolError(f"bad magic {magic!r}")
    if mtype not in NAMES:
        raise ProtocolError(f"unknown message type {mtype}")
    return mtype, length


def decode_payload(payload: bytes) -> tuple[dict, bytes]:
    """Split a frame payload into (metadata dict, blob)."""
    if len(payload) < JSONLEN.size:
        raise ProtocolError("payload too short for json header")
    (jlen,) = JSONLEN.unpack(payload[: JSONLEN.size])
    if jlen > len(payload) - JSONLEN.size:
        raise ProtocolError("json header length exceeds payload")
    try:
        meta = json.loads(payload[JSONLEN.size : JSONLEN.size + jlen].decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ProtocolError(f"bad json header: {exc}") from exc
    if not isinstance(meta, dict):
        raise ProtocolError("json header is not an object")
    return meta, payload[JSONLEN.size + jlen :]
