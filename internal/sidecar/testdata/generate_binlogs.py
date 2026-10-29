"""Generate deterministic, CRC32-checked MySQL v4 binlog fixtures."""
import pathlib
import struct
import uuid
import zlib

ROOT = pathlib.Path(__file__).parent
SID = uuid.UUID("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee").bytes


def make_binlog(previous, timestamp, marker, tag=None):
    output = bytearray(b"\xfebin")

    def event(kind, ts, body):
        size = 19 + len(body) + 4
        header = struct.pack("<IBIIIH", ts, kind, 1, size, len(output) + size, 0)
        data = header + body
        output.extend(data + struct.pack("<I", zlib.crc32(data)))

    lengths = bytearray(42 if tag else 41)
    lengths[1] = 13  # QUERY_EVENT post-header
    fde = struct.pack("<H50sIB", 4, b"8.4.0-log", timestamp - 3600, 19)
    event(15, timestamp - 3600, fde + lengths + b"\x01")
    event(35, 0, struct.pack("<Q", 1) + SID + struct.pack("<QQQ", 1, 1, previous + 1))
    if tag:
        def fixed(data):
            return b"".join(bytes([x << 1]) if x < 128 else bytes([((x & 63) << 2) | 1, 2 if x < 192 else 3]) for x in data)

        def unsigned(value):
            length = max(1, (value.bit_length() + 6) // 7)
            return ((value << length) | ((1 << (length - 1)) - 1)).to_bytes(length, "little")

        fields = [
            (0, fixed(b"\x01")), (1, fixed(SID)),
            (2, bytes([(previous + 1) << 2])),
            (3, bytes([len(tag) << 1]) + tag.encode()),
            (4, b"\x00"), (5, b"\x04"), (6, unsigned(0)),
            (8, unsigned(0)), (9, unsigned(80400)),
        ]
        body = b"".join(bytes([index << 1]) + value for index, value in fields)
        event(42, timestamp, bytes([2, (len(body) + 2) << 1, 18]) + body)
    else:
        event(33, timestamp, b"\x01" + SID + struct.pack("<Q", previous + 1) + b"\x02" + struct.pack("<QQ", 0, 1))
    query = "INSERT INTO markers VALUES ('{}')".format(marker).encode()
    event(2, timestamp, struct.pack("<IIBHH", 1, 0, 6, 0, 0) + b"orders\x00" + query)
    event(3, timestamp + 1, b"")
    return output


for name, previous, timestamp, marker in [
    ("incarnation-a.bin", 10, 1776243600, "A"),
    ("incarnation-b.bin", 20, 1776247200, "B"),
    ("same-header.bin", 10, 1776243600, "B"),
    ("review-order-a.bin", 10, 1776243600, "A"),
    ("review-order-b.bin", 11, 1776243600, "later4"),
    ("review-clock-b.bin", 11, 1776243599, "later"),
]:
    (ROOT / name).write_bytes(make_binlog(previous, timestamp, marker))

(ROOT / "tagged-transaction.bin").write_bytes(make_binlog(10, 1776243600, "T", "restore_tag"))
