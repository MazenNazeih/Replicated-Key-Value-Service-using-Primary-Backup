package kvservice

import (
	"errors"
	"fmt"
	"log"
	"math/rand"
	"net"
	"net/rpc"
	"os"
	"strconv"
	"sync"
	"syscall"
	"sysmonitor"
	"time"
)

// Debugging
const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		n, err = fmt.Printf(format, a...)
	}
	return
}

type KVServer struct {
	l           net.Listener
	dead        bool // for testing
	unreliable  bool // for testing
	id          string
	monitorClnt *sysmonitor.Client
	view        sysmonitor.View
	done        sync.WaitGroup
	finish      chan interface{}
	isprimary   bool
	isbackup    bool
	database    map[string]string
	// lastAppliedSequenceNumber int
	// currentSequenceNumber int
	operations map[int]string // maps opid with old value

	// Add your declarations here.
}

// RPC for the primary to forward a Put/PutHash to the backup
func (server *KVServer) ForwardPut(args *ForwardPutArgs, reply *PutReply) error {

	if server.dead == true {
		return errors.New("Server is dead")
	}
	if server.database == nil {
		return errors.New("Database not initialized")
	}
	if args.Args.DoHash {
		reply.PreviousValue = args.OldValue
	}

	server.database[args.Args.Key] = args.Args.Value

	return nil
}

// RPC for the primary to send the full database to a new backup
func (server *KVServer) ForwardDB(args *ForwardDBArgs, reply *ForwardDBReply) error {
	if server.dead {
		return errors.New("server is dead")
	}
	if server.database == nil {
		server.database = make(map[string]string)
	}
	// Replace the backup's database with the full copy
	for k, v := range args.Database {
		server.database[k] = v
	}
	return nil
}

func (server *KVServer) Put(args *PutArgs, reply *PutReply, op_id int) error {
	// Your code here.
	if server.isprimary == false {
		return errors.New("Not primary")
	}
	if server.dead == true {
		return errors.New("Server is dead")
	}
	if server.database == nil {
		return errors.New("Database not initialized")
	}

	previous_value := server.database[args.Key]        // eturns "" if not found
	new_value_int := hash(previous_value + args.Value) // returns the new value as uint32
	new_value_str := strconv.Itoa(int(new_value_int))
	// Forward to backup if there is one
	if server.view.Backup != "" {
		backupClnt, err := rpc.Dial("unix", server.view.Backup)
		if err == nil {
			defer backupClnt.Close()
			var backupReply PutReply
			backupArgs := ForwardPutArgs{
				Args:     args,
				OpId:     op_id,
				OldValue: previous_value,
			}
			err = backupClnt.Call("KVServer.ForwardPut", backupArgs, &backupReply)
			if err != nil {
				return err
			}
		}
	}
	// if no error during forwarding then update the database
	if args.DoHash {

		server.database[args.Key] = new_value_str
		reply.PreviousValue = previous_value
	} else {
		server.database[args.Key] = args.Value
	}

	return nil
}

func (server *KVServer) Get(args *GetArgs, reply *GetReply) error {
	// Your code here.
	return nil
}

// ping the view server periodically.
func (server *KVServer) tick() {

	// This line will give an error initially as view and err are not used.
	view, err := server.monitorClnt.Ping(server.view.Viewnum)
	if err != nil {
		server.Kill()
	}

	if view.Primary == server.id {
		server.isprimary = true
	} else if view.Backup == server.id {
		server.isbackup = true
	} else {
		server.isprimary = false
		server.isbackup = false
	}

	// detect if primary and there is a new backup
	if server.isprimary && view.Backup != server.view.Backup {

		// Forward the full db to the new backup
		backupClnt, err := rpc.Dial("unix", view.Backup)
		for err != nil {
			view, err := server.monitorClnt.Ping(server.view.Viewnum)
			if err != nil {
				server.Kill()
				break
			}
			server.view = view
			backupClnt, err = rpc.Dial("unix", server.view.Backup)
			if err != nil {
				continue
			}
			defer backupClnt.Close()
			args := &ForwardDBArgs{Database: server.database}
			var reply ForwardDBReply
			err = backupClnt.Call("KVServer.ForwardDB", args, &reply)
			if err != nil {
				continue
			}
		}

	}

}

// tell the server to shut itself down.
// please do not change this function.
func (server *KVServer) Kill() {
	server.dead = true
	server.l.Close()
}

func StartKVServer(monitorServer string, id string) *KVServer {
	server := new(KVServer)
	server.id = id
	server.monitorClnt = sysmonitor.MakeClient(id, monitorServer)
	server.view = sysmonitor.View{}
	server.finish = make(chan interface{})
	server.database = make(map[string]string)
	// Add your server initializations here
	// ==================================

	//====================================

	rpcs := rpc.NewServer()
	rpcs.Register(server)

	os.Remove(server.id)
	l, e := net.Listen("unix", server.id)
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	server.l = l

	// please do not make any changes in the following code,
	// or do anything to subvert it.

	go func() {
		for server.dead == false {
			conn, err := server.l.Accept()
			if err == nil && server.dead == false {
				if server.unreliable && (rand.Int63()%1000) < 100 {
					// discard the request.
					conn.Close()
				} else if server.unreliable && (rand.Int63()%1000) < 200 {
					// process the request but force discard of reply.
					c1 := conn.(*net.UnixConn)
					f, _ := c1.File()
					err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
					if err != nil {
						fmt.Printf("shutdown: %v\n", err)
					}
					server.done.Add(1)
					go func() {
						rpcs.ServeConn(conn)
						server.done.Done()
					}()
				} else {
					server.done.Add(1)
					go func() {
						rpcs.ServeConn(conn)
						server.done.Done()
					}()
				}
			} else if err == nil {
				conn.Close()
			}
			if err != nil && server.dead == false {
				fmt.Printf("KVServer(%v) accept: %v\n", id, err.Error())
				server.Kill()
			}
		}
		DPrintf("%s: wait until all request are done\n", server.id)
		server.done.Wait()
		// If you have an additional thread in your solution, you could
		// have it read to the finish channel to hear when to terminate.
		close(server.finish)
	}()

	server.done.Add(1)
	go func() {
		for server.dead == false {
			server.tick()
			time.Sleep(sysmonitor.PingInterval)
		}
		server.done.Done()
	}()

	return server
}
