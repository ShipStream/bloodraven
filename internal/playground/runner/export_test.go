package runner

// AssertShardsPartition lets the external test package (which can import
// the scenarios package and so see the real registry) reuse the partition
// check.
var AssertShardsPartition = assertShardsPartition
