package sidecar

import (
	"testing"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	internalmysql "github.com/shipstream/bloodraven/internal/mysql"
)

func round4PrefixEvent(kind replication.EventType) *replication.BinlogEvent {
	return &replication.BinlogEvent{Header: &replication.EventHeader{EventType: kind}, RawData: make([]byte, 19)}
}
func round4PrefixQuery(sql string) *replication.BinlogEvent {
	event := round4PrefixEvent(replication.QUERY_EVENT)
	event.Event = &replication.QueryEvent{Query: []byte(sql)}
	return event
}
func round4PrefixPayload(events ...*replication.BinlogEvent) *replication.BinlogEvent {
	event := round4PrefixEvent(replication.TRANSACTION_PAYLOAD_EVENT)
	event.Event = &replication.TransactionPayloadEvent{Events: events, Payload: []byte{1}, Size: 1, UncompressedSize: uint64(19 * len(events))}
	for _, field := range []struct {
		key   byte
		value uint64
	}{{1, 1}, {2, 0}, {3, uint64(19 * len(events))}} {
		value := mysql.PutLengthEncodedInt(field.value)
		event.RawData = append(event.RawData, field.key, byte(len(value)))
		event.RawData = append(event.RawData, value...)
	}
	event.RawData = append(event.RawData, 0, 1)
	return event
}
func TestRound4CompletedPrefixBodyForms(t *testing.T) {
	begin := round4PrefixQuery("BEGIN")
	xid := round4PrefixEvent(replication.XID_EVENT)
	commit := round4PrefixQuery("COMMIT")
	unknown := round4PrefixEvent(replication.EventType(99))
	for _, tc := range []struct {
		name     string
		events   []*replication.BinlogEvent
		complete bool
	}{
		{"native shape", []*replication.BinlogEvent{begin, round4PrefixEvent(replication.TABLE_MAP_EVENT), round4PrefixEvent(replication.WRITE_ROWS_EVENTv2), xid}, true},
		{"savepoint before XID", []*replication.BinlogEvent{begin, round4PrefixQuery("SAVEPOINT `s1`"), xid}, true},
		{"savepoint rollback", []*replication.BinlogEvent{begin, round4PrefixQuery("SAVEPOINT `s1`"), round4PrefixQuery("ROLLBACK TO SAVEPOINT `s1`"), xid}, true},
		{"payload savepoint rollback", []*replication.BinlogEvent{round4PrefixPayload(begin, round4PrefixQuery("ROLLBACK TO SAVEPOINT `s1`"), xid)}, true},
		{"line commented XA", []*replication.BinlogEvent{round4PrefixQuery("-- transaction\nXA START 'x'"), xid}, false},
		{"version commented control", []*replication.BinlogEvent{round4PrefixQuery("/*!80000 XA START 'x' */"), xid}, false},
		{"neutral Query before COMMIT", []*replication.BinlogEvent{begin, round4PrefixQuery("RELEASE SAVEPOINT `s1`"), commit}, true},
		{"analyze", []*replication.BinlogEvent{round4PrefixQuery("ANALYZE TABLE replay.markers")}, true},
		{"optimize", []*replication.BinlogEvent{round4PrefixQuery("OPTIMIZE TABLE replay.markers")}, true},
		{"flush", []*replication.BinlogEvent{round4PrefixQuery("FLUSH PRIVILEGES")}, true},
		{"password", []*replication.BinlogEvent{round4PrefixQuery("SET PASSWORD = 'new'")}, true},
		{"commented DDL", []*replication.BinlogEvent{round4PrefixQuery("/* application */ CREATE TABLE replay.other (id INT)")}, true},
		{"decoded payload XID", []*replication.BinlogEvent{round4PrefixPayload(begin, round4PrefixQuery("SAVEPOINT `s1`"), xid)}, true},
		{"decoded payload COMMIT", []*replication.BinlogEvent{round4PrefixPayload(begin, commit)}, true},
		{"interrupted", []*replication.BinlogEvent{begin, round4PrefixQuery("INSERT INTO replay.markers VALUES (1)")}, false},
		{"rollback", []*replication.BinlogEvent{begin, round4PrefixQuery("ROLLBACK"), xid}, false},
		{"XA", []*replication.BinlogEvent{round4PrefixQuery("XA START 'x'"), xid}, false},
		{"commented XA", []*replication.BinlogEvent{round4PrefixQuery("/* application */ XA START 'x'"), xid}, false},
		{"unknown", []*replication.BinlogEvent{begin, unknown, xid}, false},
		{"payload interrupted", []*replication.BinlogEvent{round4PrefixPayload(begin, round4PrefixQuery("SAVEPOINT `s1`"))}, false},
		{"payload unknown", []*replication.BinlogEvent{round4PrefixPayload(begin, unknown, xid)}, false},
		{"payload rolled back", []*replication.BinlogEvent{round4PrefixPayload(begin, round4PrefixQuery("ROLLBACK"), xid)}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := replayPrefixCollector{committed: make(internalmysql.GTIDSet)}
			gtid := round4PrefixEvent(replication.GTID_EVENT)
			gtid.Event = &replication.GTIDEvent{SID: make([]byte, 16), GNO: 11}
			c.observe(gtid)
			for _, event := range tc.events {
				c.observe(event)
			}
			if c.committed.IsEmpty() == tc.complete {
				t.Fatalf("completed=%s, want complete=%v", c.committed, tc.complete)
			}
		})
	}
}
