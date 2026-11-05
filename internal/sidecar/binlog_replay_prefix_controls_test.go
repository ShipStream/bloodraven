package sidecar

import (
	"testing"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	internalmysql "github.com/shipstream/bloodraven/internal/mysql"
)

func prefixControlEvent(kind replication.EventType) *replication.BinlogEvent {
	return &replication.BinlogEvent{Header: &replication.EventHeader{EventType: kind}, RawData: make([]byte, 19)}
}
func prefixQueryEvent(sql string) *replication.BinlogEvent {
	event := prefixControlEvent(replication.QUERY_EVENT)
	event.Event = &replication.QueryEvent{Query: []byte(sql)}
	return event
}
func prefixPayloadEvent(events ...*replication.BinlogEvent) *replication.BinlogEvent {
	event := prefixControlEvent(replication.TRANSACTION_PAYLOAD_EVENT)
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
func TestCompletedPrefixBodyForms(t *testing.T) {
	begin := prefixQueryEvent("BEGIN")
	xid := prefixControlEvent(replication.XID_EVENT)
	commit := prefixQueryEvent("COMMIT")
	unknown := prefixControlEvent(replication.EventType(99))
	for _, tc := range []struct {
		name     string
		events   []*replication.BinlogEvent
		complete bool
	}{
		{"native shape", []*replication.BinlogEvent{begin, prefixControlEvent(replication.TABLE_MAP_EVENT), prefixControlEvent(replication.WRITE_ROWS_EVENTv2), xid}, true},
		{"savepoint before XID", []*replication.BinlogEvent{begin, prefixQueryEvent("SAVEPOINT `s1`"), xid}, true},
		{"savepoint rollback", []*replication.BinlogEvent{begin, prefixQueryEvent("SAVEPOINT `s1`"), prefixQueryEvent("ROLLBACK TO SAVEPOINT `s1`"), xid}, true},
		{"payload savepoint rollback", []*replication.BinlogEvent{prefixPayloadEvent(begin, prefixQueryEvent("ROLLBACK TO SAVEPOINT `s1`"), xid)}, true},
		{"line commented XA", []*replication.BinlogEvent{prefixQueryEvent("-- transaction\nXA START 'x'"), xid}, false},
		{"version commented control", []*replication.BinlogEvent{prefixQueryEvent("/*!80000 XA START 'x' */"), xid}, false},
		{"neutral Query before COMMIT", []*replication.BinlogEvent{begin, prefixQueryEvent("RELEASE SAVEPOINT `s1`"), commit}, true},
		{"analyze", []*replication.BinlogEvent{prefixQueryEvent("ANALYZE TABLE replay.markers")}, true},
		{"optimize", []*replication.BinlogEvent{prefixQueryEvent("OPTIMIZE TABLE replay.markers")}, true},
		{"flush", []*replication.BinlogEvent{prefixQueryEvent("FLUSH PRIVILEGES")}, true},
		{"password", []*replication.BinlogEvent{prefixQueryEvent("SET PASSWORD = 'new'")}, true},
		{"commented DDL", []*replication.BinlogEvent{prefixQueryEvent("/* application */ CREATE TABLE replay.other (id INT)")}, true},
		{"decoded payload XID", []*replication.BinlogEvent{prefixPayloadEvent(begin, prefixQueryEvent("SAVEPOINT `s1`"), xid)}, true},
		{"decoded payload COMMIT", []*replication.BinlogEvent{prefixPayloadEvent(begin, commit)}, true},
		{"interrupted", []*replication.BinlogEvent{begin, prefixQueryEvent("INSERT INTO replay.markers VALUES (1)")}, false},
		{"rollback", []*replication.BinlogEvent{begin, prefixQueryEvent("ROLLBACK"), xid}, false},
		{"XA", []*replication.BinlogEvent{prefixQueryEvent("XA START 'x'"), xid}, false},
		{"commented XA", []*replication.BinlogEvent{prefixQueryEvent("/* application */ XA START 'x'"), xid}, false},
		{"unknown", []*replication.BinlogEvent{begin, unknown, xid}, false},
		{"payload interrupted", []*replication.BinlogEvent{prefixPayloadEvent(begin, prefixQueryEvent("SAVEPOINT `s1`"))}, false},
		{"payload unknown", []*replication.BinlogEvent{prefixPayloadEvent(begin, unknown, xid)}, false},
		{"payload rolled back", []*replication.BinlogEvent{prefixPayloadEvent(begin, prefixQueryEvent("ROLLBACK"), xid)}, false},
		{"start transaction", []*replication.BinlogEvent{prefixQueryEvent("START TRANSACTION"), prefixQueryEvent("INSERT INTO replay.markers VALUES (1)"), xid}, true},
		{"standalone start", []*replication.BinlogEvent{prefixQueryEvent("START REPLICA")}, true},
		{"rollback work to savepoint", []*replication.BinlogEvent{begin, prefixQueryEvent("ROLLBACK WORK TO SAVEPOINT `s1`"), xid}, true},
		{"savepoint rollback outside transaction", []*replication.BinlogEvent{prefixQueryEvent("ROLLBACK TO SAVEPOINT `s1`"), xid}, false},
		{"hash comment", []*replication.BinlogEvent{prefixQueryEvent("# note\nCREATE TABLE replay.other (id INT)")}, true},
		{"unterminated comment", []*replication.BinlogEvent{prefixQueryEvent("/* note CREATE TABLE replay.other (id INT)"), xid}, false},
		{"unterminated line comment", []*replication.BinlogEvent{prefixQueryEvent("-- only a comment"), xid}, false},
		{"empty query", []*replication.BinlogEvent{prefixQueryEvent("  ;"), xid}, false},
		{"query event without body", []*replication.BinlogEvent{prefixControlEvent(replication.QUERY_EVENT), xid}, false},
		{"payload XA", []*replication.BinlogEvent{prefixPayloadEvent(prefixQueryEvent("XA START 'x'"), xid)}, false},
		{"payload ending without commit", []*replication.BinlogEvent{prefixPayloadEvent(begin, prefixControlEvent(replication.WRITE_ROWS_EVENTv2))}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := replayPrefixCollector{committed: make(internalmysql.GTIDSet)}
			gtid := prefixControlEvent(replication.GTID_EVENT)
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
