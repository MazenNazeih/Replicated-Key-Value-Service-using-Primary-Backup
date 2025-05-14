package kvservice

import (
	"errors"
	"fmt"
	"log"
	"math/rand"
	"net"
	"net/rpc"
	"os"
	"replicatedkv/sysmonitor"
	"strconv"
	"sync"
	"syscall"
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
	PutOps map[string]PutReply
	GetOps map[string]GetReply
	mu     sync.Mutex
	// Add your declarations here.
}

// RPC for the primary to forward a Put/PutHash to the backup
func (server *KVServer) ForwardPut(args *ForwardPutArgs, reply *PutReply) error {
	server.mu.Lock()
	defer server.mu.Unlock()

	// fmt.Printf("forwarding value  %s\n", args.Args.Value)

	if server.dead {
		return errors.New("server is dead")
	}
	if server.database == nil {
		return errors.New("database not initialized")
	}

	if r, ok := server.PutOps[args.Args.OpId]; ok {
		reply.PreviousValue = r.PreviousValue
		reply.Err = OK
		return nil
	}

	if args.Args.DoHash {
		reply.PreviousValue = args.OldValue

	}

	server.database[args.Args.Key] = args.Args.Value
	server.PutOps[args.Args.OpId] = PutReply{
		Err:           OK,
		PreviousValue: args.OldValue,
	}

	return nil
}

// RPC for the primary to send the full database to a new backup
func (server *KVServer) ForwardDB(args *ForwardDBArgs, reply *ForwardDBReply) error {

	server.mu.Lock()
	defer server.mu.Unlock()

	if server.dead {
		return errors.New("server is dead")
	}
	if server.database == nil {
		server.database = make(map[string]string)
	}
	if server.PutOps == nil {
		server.PutOps = make(map[string]PutReply)
	}
	if server.GetOps == nil {
		server.GetOps = make(map[string]GetReply)
	}
	// Replace the backup's database with the full copy
	for k, v := range args.Database {
		server.database[k] = v
	}
	for k, v := range args.PutOps {
		server.PutOps[k] = v
	}
	for k, v := range args.GetOps {
		server.GetOps[k] = v
	}

	return nil
}

func (server *KVServer) Put(args *PutArgs, reply *PutReply) error {
	// Your code here.
	server.mu.Lock()
	defer server.mu.Unlock()

	if !server.isprimary {
		return errors.New("not primary")
	}
	if server.dead {
		return errors.New("server is dead")
	}
	if server.database == nil {
		return errors.New("database not initialized")
	}

	if r, ok := server.PutOps[args.OpId]; ok {
		reply.PreviousValue = r.PreviousValue
		reply.Err = OK
		return nil
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
			backupArgs := &ForwardPutArgs{
				Args:     args,
				OldValue: previous_value,
			}
			err = backupClnt.Call("KVServer.ForwardPut", backupArgs, &backupReply)
			if err != nil {
				return err
			}
		} else {
			return err
		}
	}
	// if no error during forwarding then update the database
	if args.DoHash {

		server.database[args.Key] = new_value_str
		reply.PreviousValue = previous_value
		server.PutOps[args.OpId] = PutReply{
			Err:           OK,
			PreviousValue: previous_value,
		}
	} else {
		server.database[args.Key] = args.Value
		server.PutOps[args.OpId] = PutReply{
			Err:           OK,
			PreviousValue: previous_value,
		}
	}

	return nil
}

func (server *KVServer) Get(args *GetArgs, reply *GetReply) error {
	// Your code here.
	server.mu.Lock()
	defer server.mu.Unlock()

	if !server.isprimary {
		return errors.New("not primary")
	}
	if server.dead {
		return errors.New("server is dead")
	}
	if server.database == nil {
		return errors.New("database not initialized")
	}

	if r, ok := server.GetOps[args.OpId]; ok {
		reply.Value = r.Value
		reply.Err = OK
		return nil
	}

	val, ok := server.database[args.Key]
	if ok {
		reply.Value = val
		reply.Err = OK
	} else {
		reply.Value = ""
		reply.Err = ErrNoKey
	}

	server.GetOps[args.OpId] = *reply
	return nil

}

// ping the view server periodically.
func (server *KVServer) tick() {

	// This line will give an error initially as view and err are not used.
	view, err := server.monitorClnt.Ping(server.view.Viewnum)

	// Your code here.
	// loop until we get a valid view
	for err != nil {
		view, err = server.monitorClnt.Ping(server.view.Viewnum)
	}

	server.mu.Lock()
	defer server.mu.Unlock()

	if view.Primary == server.id {
		server.isprimary = true
		server.isbackup = false
	} else if view.Backup == server.id {
		server.isbackup = true
		server.isprimary = false
	} else {
		server.isprimary = false
		server.isbackup = false
	}

	// check if primary has new backup
	if server.isprimary && view.Backup != server.view.Backup && view.Backup != "" {
		args := &ForwardDBArgs{
			Database: server.database,
			PutOps:   server.PutOps,
			GetOps:   server.GetOps,
		}
		var reply ForwardDBReply
		ok := call(view.Backup, "KVServer.ForwardDB", args, &reply)
		for !ok {
			// check tte view again
			view, err = server.monitorClnt.Ping(server.view.Viewnum)
			if err != nil {
				continue
			}
			if view.Backup != "" {
				ok = call(view.Backup, "KVServer.ForwardDB", args, &reply)
			} else {
				break
			}

		}

	}

	server.view = view
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
	server.GetOps = make(map[string]GetReply)
	server.PutOps = make(map[string]PutReply)

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
