// Package pb holds the wire definitions and their generated code.
package pb

//go:generate protoc --go_out=. --go_opt=paths=source_relative --go-grpc_out=. --go-grpc_opt=paths=source_relative raft.proto kv.proto
