package sysmonitor

import "net/rpc"
import "fmt"

type Client struct {
	me     string // client's name
	server string // the server of the monitor service
}

func MakeClient(me string, server string) *Client {
	ck := new(Client)
	ck.me = me
	ck.server = server
	return ck
}

//
// call() sends an RPC to the rpcname handler on server srv
// with arguments args, waits for the reply, and leaves the
// reply in reply. the reply argument should be a pointer
// to a reply structure.
//
// the return value is true if the server responded, and false
// if call() was not able to contact the server. in particular,
// the reply's contents are only valid if call() returned true.
//
// you should assume that call() will time out and return an
// error after a while if it doesn't get a reply from the server.
//
// please use call() to send all RPCs, in client.go and server.go.
// please don't change this function.
//
func call(srv string, rpcname string,
	args interface{}, reply interface{}) bool {
	c, errx := rpc.Dial("unix", srv)
	if errx != nil {
		return false
	}
	defer c.Close()

	err := c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}

func (ck *Client) Ping(viewnum uint) (View, error) {
	// prepare the arguments.
	args := &PingArgs{}
	args.Me = ck.me
	args.Viewnum = viewnum
	var reply PingReply

	// send an RPC request, wait for the reply.
	ok := call(ck.server, "MonitorServer.Ping", args, &reply)
	if ok == false {
		return View{}, fmt.Errorf("Ping(%v) failed", viewnum)
	}

	return reply.View, nil
}

func (ck *Client) Get() (View, bool) {
	args := &GetArgs{}
	var reply GetReply
	ok := call(ck.server, "MonitorServer.Get", args, &reply)
	if ok == false {
		return View{}, false
	}
	return reply.View, true
}

func (ck *Client) Primary() string {
	v, ok := ck.Get()
	if ok {
		return v.Primary
	}
	return ""
}
