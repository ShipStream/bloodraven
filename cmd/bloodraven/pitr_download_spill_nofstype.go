//go:build !linux

package main

// stagingInMemory: without Linux's statfs the staging volume's kind is
// unknown, and the spill arena is not implemented either.
func stagingInMemory(string) bool { return true }
