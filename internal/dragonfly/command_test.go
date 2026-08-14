package dragonfly

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"
)

func TestHasCommand(t *testing.T) {
	present := "*1\r\n*2\r\n$12\r\nREPLTAKEOVER\r\n:-2\r\n"
	nullArray := "*1\r\n*-1\r\n"
	nullBulk := "*1\r\n$-1\r\n"
	unknown := "-ERR unknown command `COMMAND`, with args beginning with: \r\n"
	unfiltered := "*2\r\n*2\r\n$4\r\nPING\r\n:1\r\n*2\r\n$12\r\nREPLTAKEOVER\r\n:-2\r\n"

	tests := []struct {
		name    string
		reply   string
		want    bool
		wantErr bool
	}{
		{name: "present filtered", reply: present, want: true},
		{name: "present unfiltered table", reply: unfiltered, want: true},
		{name: "missing null array", reply: nullArray, want: false},
		{name: "missing null bulk", reply: nullBulk, want: false},
		{name: "unknown command verb", reply: unknown, want: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			srv := newFakeServer(t)
			srv.reply("COMMAND INFO REPLTAKEOVER", tt.reply)
			c := mustNewClient(t, srv.addr(), "")
			defer c.Close()
			got, err := c.HasCommand(context.Background(), "REPLTAKEOVER")
			if (err != nil) != tt.wantErr {
				t.Fatalf("HasCommand err=%v wantErr=%v", err, tt.wantErr)
			}
			if got != tt.want {
				t.Errorf("HasCommand = %v, want %v", got, tt.want)
			}
			cmd := <-srv.commandsCh
			if want := []string{"COMMAND", "INFO", "REPLTAKEOVER"}; !equalSlices(cmd, want) {
				t.Errorf("sent %v, want %v", cmd, want)
			}
		})
	}
}

func TestHasCommandFallsBackToBareCommand(t *testing.T) {
	srv := newFakeServer(t)
	srv.reply("COMMAND INFO REPLTAKEOVER", "-ERR syntax error\r\n")
	srv.reply("COMMAND", "*2\r\n*2\r\n$4\r\nPING\r\n:1\r\n*2\r\n$12\r\nREPLTAKEOVER\r\n:-2\r\n")
	c := mustNewClient(t, srv.addr(), "")
	defer c.Close()
	got, err := c.HasCommand(context.Background(), "REPLTAKEOVER")
	if err != nil {
		t.Fatalf("HasCommand: %v", err)
	}
	if !got {
		t.Fatal("expected fallback COMMAND table to report REPLTAKEOVER present")
	}
}

func TestHasCommandFallsBackToBareCommandMissing(t *testing.T) {
	srv := newFakeServer(t)
	srv.reply("COMMAND INFO REPLTAKEOVER", "-ERR unknown subcommand 'INFO'\r\n")
	srv.reply("COMMAND", "*1\r\n*2\r\n$4\r\nPING\r\n:1\r\n")
	c := mustNewClient(t, srv.addr(), "")
	defer c.Close()
	got, err := c.HasCommand(context.Background(), "REPLTAKEOVER")
	if err != nil {
		t.Fatalf("HasCommand: %v", err)
	}
	if got {
		t.Fatal("expected missing REPLTAKEOVER in fallback COMMAND table")
	}
}

func TestHasCommandIOError(t *testing.T) {
	srv := newFakeServer(t)
	c := mustNewClient(t, srv.addr(), "")
	_ = c.Close()
	got, err := c.HasCommand(context.Background(), "REPLTAKEOVER")
	if err == nil {
		t.Fatal("expected error after Close")
	}
	if got {
		t.Errorf("HasCommand = true on I/O error; must not claim supported")
	}
}

func TestCommandInfoReports(t *testing.T) {
	tests := []struct {
		name string
		raw  string
		want bool
	}{
		{name: "present", raw: "*1\r\n*2\r\n$12\r\nREPLTAKEOVER\r\n:-2\r\n", want: true},
		{name: "null array slot", raw: "*1\r\n*-1\r\n", want: false},
		{name: "null bulk slot", raw: "*1\r\n$-1\r\n", want: false},
		{name: "empty array", raw: "*0\r\n", want: false},
		{name: "other command only", raw: "*1\r\n*2\r\n$4\r\nPING\r\n:1\r\n", want: false},
		{name: "top-level null bulk", raw: "$-1\r\n", want: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			v, err := readValue(bufio.NewReader(bytes.NewReader([]byte(tt.raw))), 0)
			if err != nil {
				t.Fatalf("readValue: %v", err)
			}
			if got := commandInfoReports(v, "REPLTAKEOVER"); got != tt.want {
				t.Errorf("commandInfoReports = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestReadValueCaps(t *testing.T) {
	t.Run("array length", func(t *testing.T) {
		raw := []byte("*1025\r\n")
		_, err := readValue(bufio.NewReader(bytes.NewReader(raw)), 0)
		if err == nil || !strings.Contains(err.Error(), "exceeds cap") {
			t.Fatalf("want array length cap error, got %v", err)
		}
	})
	t.Run("nesting", func(t *testing.T) {
		var b bytes.Buffer
		for i := 0; i < MaxArrayDepth+2; i++ {
			b.WriteString("*1\r\n")
		}
		b.WriteString("+ok\r\n")
		_, err := readValue(bufio.NewReader(&b), 0)
		if err == nil || !strings.Contains(err.Error(), "nesting exceeds") {
			t.Fatalf("want nesting cap error, got %v", err)
		}
	})
}

func TestIsUnknownCommand(t *testing.T) {
	tests := []struct {
		err  error
		want bool
	}{
		{err: &ServerError{Message: "ERR unknown command 'REPLTAKEOVER'"}, want: true},
		{err: &ServerError{Message: "ERR unknown command `COMMAND`, with args beginning with:"}, want: true},
		{err: &ServerError{Message: "ERR wrong number of arguments for 'repltakeover' command"}, want: false},
		{err: &ServerError{Message: "NOAUTH Authentication required."}, want: false},
		{err: errors.New("connection reset"), want: false},
		{err: nil, want: false},
	}
	for _, tt := range tests {
		if got := IsUnknownCommand(tt.err); got != tt.want {
			t.Errorf("IsUnknownCommand(%v) = %v, want %v", tt.err, got, tt.want)
		}
	}
}
