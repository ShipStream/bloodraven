//go:build ignore

// Generate deterministic empty and compressed retention fixtures after generate_binlogs.py.
package main

import (
	"encoding/binary"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/klauspost/compress/zstd"
	"hash/crc32"
	"os"
)

func main() {
	data, err := os.ReadFile("internal/sidecar/testdata/incarnation-a.bin")
	if err != nil {
		panic(err)
	}
	var events [][]byte
	for p := 4; p < len(data); {
		size := int(binary.LittleEndian.Uint32(data[p+9 : p+13]))
		events = append(events, append([]byte{}, data[p:p+size]...))
		p += size
	}
	event := func(raw []byte, typ byte, body []byte, pos int) []byte {
		header := append([]byte{}, raw[:19]...)
		header[4] = typ
		size := 19 + len(body) + 4
		binary.LittleEndian.PutUint32(header[9:13], uint32(size))
		binary.LittleEndian.PutUint32(header[13:17], uint32(pos+size))
		out := append(header, body...)
		return binary.LittleEndian.AppendUint32(out, crc32.ChecksumIEEE(out))
	}
	prefix := append(append([]byte{}, data[:4]...), append(events[0], events[1]...)...)
	empty := append(append([]byte{}, prefix...), event(events[4], 3, nil, len(prefix))...)
	if err := os.WriteFile("internal/sidecar/testdata/empty-sealed.bin", empty, 0600); err != nil {
		panic(err)
	}
	inner := append([]byte{}, events[3][:len(events[3])-4]...)
	binary.LittleEndian.PutUint32(inner[9:13], uint32(len(inner)))
	binary.LittleEndian.PutUint32(inner[13:17], 0)
	enc, err := zstd.NewWriter(nil, zstd.WithEncoderConcurrency(1))
	if err != nil {
		panic(err)
	}
	payload := enc.EncodeAll(inner, nil)
	enc.Close()
	body := []byte{}
	for _, field := range []struct {
		key   byte
		value uint64
	}{{1, uint64(len(payload))}, {2, 0}, {3, uint64(len(inner))}} {
		value := mysql.PutLengthEncodedInt(field.value)
		body = append(body, field.key, byte(len(value)))
		body = append(body, value...)
	}
	body = append(body, 0)
	body = append(body, payload...)
	compressed := append(append([]byte{}, prefix...), events[2]...)
	compressed = append(compressed, event(events[3], 40, body, len(compressed))...)
	compressed = append(compressed, event(events[4], 3, nil, len(compressed))...)
	if err := os.WriteFile("internal/sidecar/testdata/compressed-transaction.bin", compressed, 0600); err != nil {
		panic(err)
	}
}
