package kvservice

import (
	"crypto/rand"
	"hash/fnv"
	"math/big"
)

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongServer = "ErrWrongServer"
)

type Err string

type PutArgs struct {
	Key    string
	Value  string
	DoHash bool // For PutHash
	// Add your definitions here.
	OpId string // OpId is a unique identifier for the operation
	// Field names should start with capital letters for RPC to work.
}

type PutReply struct {
	Err           Err
	PreviousValue string // For PutHash
}

type GetArgs struct {
	Key string
	// Add your definitions here.
	OpId string // OpId is a unique identifier for the operation
}

type GetReply struct {
	Err   Err
	Value string
}

// Add your RPC definitions here.
//======================================

type ForwardDBArgs struct {
	Database map[string]string
	PutOps     map[string]PutReply
	GetOps    map[string]GetReply
}
type ForwardDBReply struct{}

type ForwardPutArgs struct {
	Args     *PutArgs
	OldValue string
}

// ======================================

func hash(s string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(s))
	return h.Sum32()
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}
