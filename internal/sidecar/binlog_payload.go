package sidecar

import (
	"encoding/binary"
	"fmt"

	"github.com/go-mysql-org/go-mysql/replication"
)

// MySQL writes payload TLV values with net_store_length. go-mysql decodes the
// inner events correctly, but reads these values as fixed-width integers.
func decodedTransactionPayload(event *replication.BinlogEvent) (*replication.TransactionPayloadEvent, error) {
	payload, ok := event.Event.(*replication.TransactionPayloadEvent)
	if !ok || payload == nil || len(event.RawData) < replication.EventHeaderSize {
		return nil, fmt.Errorf("missing decoded transaction payload")
	}
	data := event.RawData[replication.EventHeaderSize:]
	var values [4]uint64
	seen := uint64(0)
	for {
		field, n, err := payloadLengthEncodedInt(data)
		if err != nil {
			return nil, err
		}
		data = data[n:]
		if field == 0 {
			break
		}
		if field > 3 || seen&(1<<field) != 0 {
			return nil, fmt.Errorf("unknown or duplicate payload field %d", field)
		}
		length, n, err := payloadLengthEncodedInt(data)
		if err != nil {
			return nil, err
		}
		data = data[n:]
		if length > uint64(len(data)) {
			return nil, fmt.Errorf("truncated payload header value")
		}
		value, n, err := payloadLengthEncodedInt(data[:int(length)])
		if err != nil || uint64(n) != length {
			return nil, fmt.Errorf("invalid payload header value for field %d", field)
		}
		values[field] = value
		seen |= 1 << field
		data = data[int(length):]
	}
	if seen != 14 {
		return nil, fmt.Errorf("incomplete payload header")
	}
	corrected := *payload
	corrected.Size = values[1]
	corrected.CompressionType = values[2]
	corrected.UncompressedSize = values[3]
	if corrected.CompressionType != replication.ZSTD || corrected.Size != uint64(len(corrected.Payload)) {
		return nil, fmt.Errorf("unsupported or inconsistent compressed payload")
	}
	var size uint64
	for _, inner := range corrected.Events {
		if inner == nil || inner.Header == nil {
			return nil, fmt.Errorf("missing decoded inner event")
		}
		size += uint64(len(inner.RawData))
	}
	if len(corrected.Events) == 0 || size != corrected.UncompressedSize {
		return nil, fmt.Errorf("incomplete decoded transaction payload")
	}
	return &corrected, nil
}

func payloadLengthEncodedInt(data []byte) (uint64, int, error) {
	if len(data) == 0 {
		return 0, 0, fmt.Errorf("truncated length-encoded integer")
	}
	width := 0
	switch data[0] {
	case 0xfc:
		width = 2
	case 0xfd:
		width = 3
	case 0xfe:
		width = 8
	case 0xfb, 0xff:
		return 0, 0, fmt.Errorf("invalid length-encoded integer")
	default:
		return uint64(data[0]), 1, nil
	}
	if len(data) < width+1 {
		return 0, 0, fmt.Errorf("truncated length-encoded integer")
	}
	var padded [8]byte
	copy(padded[:], data[1:width+1])
	return binary.LittleEndian.Uint64(padded[:]), width + 1, nil
}
