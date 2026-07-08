package controller

import "testing"

func TestMySQLTLSConfigNameDeterministicAndRotates(t *testing.T) {
	data := map[string][]byte{
		"ca.crt":  []byte("ca"),
		"tls.crt": []byte("cert"),
		"tls.key": []byte("key"),
	}
	name1 := mysqlTLSConfigName("ns", "orders", "mysql-orders-iad.ns.svc.cluster.local", data)
	name2 := mysqlTLSConfigName("ns", "orders", "mysql-orders-iad.ns.svc.cluster.local", data)
	if name1 != name2 {
		t.Fatalf("same inputs produced different names: %q vs %q", name1, name2)
	}
	name3 := mysqlTLSConfigName("ns", "orders", "mysql-orders-pdx.ns.svc.cluster.local", data)
	if name1 == name3 {
		t.Fatal("different ServerName should produce different TLS config name")
	}
	data["ca.crt"] = []byte("rotated-ca")
	name4 := mysqlTLSConfigName("ns", "orders", "mysql-orders-iad.ns.svc.cluster.local", data)
	if name1 == name4 {
		t.Fatal("rotated CA should produce different TLS config name")
	}
}

func TestMySQLServiceHosts(t *testing.T) {
	if got, want := siteServiceHost("orders", "iad", "shop"), "mysql-orders-iad.shop.svc.cluster.local"; got != want {
		t.Fatalf("siteServiceHost() = %q, want %q", got, want)
	}
	if got, want := primaryServiceHost("orders", "shop"), "mysql-orders-primary.shop.svc.cluster.local"; got != want {
		t.Fatalf("primaryServiceHost() = %q, want %q", got, want)
	}
}
