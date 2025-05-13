package sysmonitor

import (
	"fmt"
	"log"
	"net"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type MonitorServer struct {
	mu   sync.Mutex
	l    net.Listener
	dead bool
	me   string

	currentView View
	currentTick uint

	primaryViewNum  uint
	primaryLastPing uint
	backupViewNum   uint
	backupLastPing  uint
}

func (vs *MonitorServer) currentViewAcked() bool {
	return vs.currentView.Viewnum == vs.primaryViewNum
}

// server Ping RPC handler.
func (vs *MonitorServer) Ping(args *PingArgs, reply *PingReply) error {

	clientViewNum := args.Viewnum
	clientName := args.Me
	vs.mu.Lock()

	switch {

	case vs.currentView.Primary == "" && vs.currentView.Viewnum == 0:

		vs.currentView.Primary = clientName
		vs.currentView.Viewnum = 1
		vs.primaryLastPing = vs.currentTick
		vs.primaryViewNum = 0
	case vs.currentView.Primary == clientName:
		if clientViewNum != 0 {
			vs.primaryViewNum = clientViewNum
			vs.primaryLastPing = vs.currentTick
		} else {
			if vs.currentViewAcked() {
				vs.currentView.Primary = vs.currentView.Backup
				vs.currentView.Backup = ""
				vs.currentView.Viewnum++
				vs.primaryViewNum = vs.backupViewNum
				vs.primaryLastPing = vs.backupLastPing
			}
		}
	case vs.currentView.Backup == "" && vs.currentViewAcked():
		vs.currentView.Viewnum++
		vs.currentView.Backup = clientName
		vs.backupLastPing = vs.currentTick
	case vs.currentView.Backup == clientName:
		vs.backupLastPing = vs.currentTick
	}
	reply.View = vs.currentView
	vs.mu.Unlock()

	return nil
}

func (vs *MonitorServer) Get(args *GetArgs, reply *GetReply) error {

	vs.mu.Lock()

	reply.View = vs.currentView
	vs.mu.Unlock()

	return nil
}

// tick() is called once per PingInterval; it should notice
// if servers have died or recovered, and change the view
// accordingly.
func (vs *MonitorServer) tick() {

	vs.mu.Lock()

	vs.currentTick++
	if vs.currentViewAcked() {

		if vs.currentTick-vs.primaryLastPing >= DeadPings {

			if vs.currentView.Backup != "" {

				vs.currentView.Primary = vs.currentView.Backup
				vs.currentView.Backup = ""
				vs.currentView.Viewnum++
				vs.primaryViewNum = vs.backupViewNum
				vs.primaryLastPing = vs.backupLastPing
			}
		}

		if vs.currentView.Backup != "" && vs.currentTick-vs.backupLastPing >= DeadPings {
			vs.currentView.Backup = ""
			vs.currentView.Viewnum++
		}

	}
	vs.mu.Unlock()

}

func (vs *MonitorServer) Kill() {
	vs.dead = true
	vs.l.Close()
}

func StartServer(me string) *MonitorServer {
	vs := new(MonitorServer)
	vs.me = me

	vs.currentView = View{0, "", ""}
	vs.primaryViewNum = 0
	vs.backupViewNum = 0
	vs.primaryLastPing = 0
	vs.backupLastPing = 0

	rpcs := rpc.NewServer()
	rpcs.Register(vs)

	// prepare to receive connections from clients.
	// change "unix" to "tcp" to use over a network.
	os.Remove(vs.me) // only needed for "unix"
	l, e := net.Listen("unix", vs.me)
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	vs.l = l

	// create a thread to accept RPC connections from clients.
	go func() {
		for vs.dead == false {
			conn, err := vs.l.Accept()
			if err == nil && vs.dead == false {
				go rpcs.ServeConn(conn)
			} else if err == nil {
				conn.Close()
			}
			if err != nil && vs.dead == false {
				fmt.Printf("MonitorServer(%v) accept: %v\n", me, err.Error())
				vs.Kill()
			}
		}
	}()

	// create a thread to call tick() periodically.
	go func() {
		for vs.dead == false {
			vs.tick()
			time.Sleep(PingInterval)
		}
	}()

	return vs
}
