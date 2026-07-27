package sidecar

import (
	"database/sql"
	"testing"
)

func TestMySQLBool(t *testing.T) {
	tests := []struct {
		name  string
		value sql.NullString
		want  bool
	}{
		{name: "null", value: sql.NullString{}, want: false},
		{name: "zero", value: sql.NullString{String: "0", Valid: true}, want: false},
		{name: "one", value: sql.NullString{String: "1", Valid: true}, want: true},
		{name: "off", value: sql.NullString{String: "OFF", Valid: true}, want: false},
		{name: "on", value: sql.NullString{String: "On", Valid: true}, want: true},
		{name: "true", value: sql.NullString{String: " true ", Valid: true}, want: true},
		{name: "unknown", value: sql.NullString{String: "yes", Valid: true}, want: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := mysqlBool(tt.value); got != tt.want {
				t.Fatalf("mysqlBool(%q) = %v, want %v", tt.value.String, got, tt.want)
			}
		})
	}
}
