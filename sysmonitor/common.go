package sysmonitor

import "time"

type View struct {
	Viewnum uint
	Primary string
	Backup  string
}

const PingInterval = time.Millisecond * 100

// the service will declare a client dead if it misses
// this many Ping RPCs in a row.
const DeadPings = 5

type PingArgs struct {
	Me      string
	Viewnum uint // caller's notion of current view #
}

type PingReply struct {
	View View
}

//
// Get(): fetch the current view, without volunteering
// to be a server. mostly for clients of the p/b service,
// and for testing.
//

type GetArgs struct {
}

type GetReply struct {
	View View
}
