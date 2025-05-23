package kvservice

import (
	"fmt"
	"io"
	"log"
	"math/rand"
	"net"
	"os"
	"replicatedkv/sysmonitor"
	"runtime"
	"strconv"
	"testing"
	"time"
)

func check(ck *KVClient, key string, value string) {
	v := ck.Get(key)
	if v != value {
		log.Fatalf("Get(%v) -> %v, expected %v", key, v, value)
	}
}

func port(tag string, host int) string {
	s := "/var/tmp/824-"
	s += strconv.Itoa(os.Getuid()) + "/"
	os.Mkdir(s, 0777)
	s += "pb-"
	s += strconv.Itoa(os.Getpid()) + "-"
	s += tag + "-"
	s += strconv.Itoa(host)
	return s
}

var count int = 0

func TestBasicFail(t *testing.T) {
	runtime.GOMAXPROCS(4)

	tag := "basic"
	count++
	vshost := port(tag+"v", count)
	vs := sysmonitor.StartServer(vshost)
	time.Sleep(time.Second)
	vck := sysmonitor.MakeClient("", vshost)

	ck := MakeKVClient(vshost)

	fmt.Printf("Test: Single primary, no backup ...\n")

	s1 := StartKVServer(vshost, port(tag, 1))

	deadtime := sysmonitor.PingInterval * sysmonitor.DeadPings
	time.Sleep(deadtime * 2)

	if vck.Primary() != s1.id {
		t.Fatal("first primary never formed view")
	}

	ck.Put("111", "v1")
	check(ck, "111", "v1")

	ck.Put("2", "v2")
	check(ck, "2", "v2")

	ck.Put("1", "v1a")
	check(ck, "1", "v1a")

	fmt.Printf("  ... Passed\n")

	// add a backup

	fmt.Printf("Test: Add a backup ...\n")

	s2 := StartKVServer(vshost, port(tag, 2))
	for i := 0; i < sysmonitor.DeadPings*2; i++ {
		v, _ := vck.Get()
		if v.Backup == s2.id {
			break
		}
		time.Sleep(sysmonitor.PingInterval)
	}
	v, _ := vck.Get()
	if v.Backup != s2.id {
		t.Fatal("backup never came up")
	}

	ck.Put("3", "33")
	check(ck, "3", "33")

	// give the backup time to initialize
	time.Sleep(3 * sysmonitor.PingInterval)

	ck.Put("4", "44")
	check(ck, "4", "44")

	fmt.Printf("  ... Passed\n")

	// kill the primary

	fmt.Printf("Test: Primary failure ...\n")

	s1.Kill()
	for i := 0; i < sysmonitor.DeadPings*2; i++ {
		v, _ := vck.Get()
		if v.Primary == s2.id {
			break
		}
		time.Sleep(sysmonitor.PingInterval)
	}
	v, _ = vck.Get()
	if v.Primary != s2.id {
		t.Fatal("backup never switched to primary")
	}

	check(ck, "1", "v1a")
	check(ck, "3", "33")
	check(ck, "4", "44")

	fmt.Printf("  ... Passed\n")

	// kill solo server, start new server, check that
	// it does not start serving as primary

	fmt.Printf("Test: Kill last server, new one should not be active ...\n")

	s2.Kill()
	s3 := StartKVServer(vshost, port(tag, 3))
	time.Sleep(1 * time.Second)
	get_done := false
	go func() {
		ck.Get("1")
		get_done = true
	}()
	time.Sleep(2 * time.Second)
	if get_done {
		t.Fatalf("ck.Get() returned even though no initialized primary")
	}

	fmt.Printf("  ... Passed\n")

	s1.Kill()
	s2.Kill()
	s3.Kill()
	time.Sleep(time.Second)
	vs.Kill()
	time.Sleep(time.Second)
}

func TestAtMostOnce(t *testing.T) {
	runtime.GOMAXPROCS(4)

	tag := "csu"
	count++
	vshost := port(tag+"v", count)
	vs := sysmonitor.StartServer(vshost)
	time.Sleep(time.Second)
	vck := sysmonitor.MakeClient("", vshost)

	fmt.Printf("Test: at-most-once Put; unreliable ...\n")

	const nservers = 1
	var sa [nservers]*KVServer
	for i := 0; i < nservers; i++ {
		sa[i] = StartKVServer(vshost, port(tag, i+1))
		sa[i].unreliable = true
	}

	for iters := 0; iters < sysmonitor.DeadPings*2; iters++ {
		view, _ := vck.Get()
		if view.Primary != "" && view.Backup != "" {
			break
		}
		time.Sleep(sysmonitor.PingInterval)
	}

	// give p+b time to ack, initialize
	time.Sleep(sysmonitor.PingInterval * sysmonitor.DeadPings)

	ck := MakeKVClient(vshost)
	k := "counter"
	val := ""
	for i := 0; i < 100; i++ {
		v := strconv.Itoa(i)
		pv := ck.PutHash(k, v)
		if pv != val {
			t.Fatalf("ck.Puthash() returned %v but expected %v\n", pv, val)
		}
		h := hash(val + v)
		val = strconv.Itoa(int(h))
	}

	v := ck.Get(k)
	if v != val {
		t.Fatalf("ck.Get() returned %v but expected %v\n", v, val)
	}

	fmt.Printf("  ... Passed\n")

	for i := 0; i < nservers; i++ {
		sa[i].Kill()
	}
	time.Sleep(time.Second)
	vs.Kill()
	time.Sleep(time.Second)
}

// Put right after a backup dies.
func TestFailPut(t *testing.T) {
	runtime.GOMAXPROCS(4)

	tag := "failput"
	count++
	vshost := port(tag+"v", count)
	vs := sysmonitor.StartServer(vshost)
	time.Sleep(time.Second)
	vck := sysmonitor.MakeClient("", vshost)

	s1 := StartKVServer(vshost, port(tag, 1))
	time.Sleep(time.Second)
	s2 := StartKVServer(vshost, port(tag, 2))
	time.Sleep(time.Second)
	s3 := StartKVServer(vshost, port(tag, 3))

	for i := 0; i < sysmonitor.DeadPings*3; i++ {
		v, _ := vck.Get()
		if v.Primary != "" && v.Backup != "" {
			break
		}
		time.Sleep(sysmonitor.PingInterval)
	}
	time.Sleep(time.Second) // wait for backup initializion
	v1, _ := vck.Get()
	if v1.Primary != s1.id || v1.Backup != s2.id {
		t.Fatalf("wrong primary or backup")
	}

	ck := MakeKVClient(vshost)

	ck.Put("a", "aa")
	ck.Put("b", "bb")
	ck.Put("c", "cc")
	check(ck, "a", "aa")
	check(ck, "b", "bb")
	check(ck, "c", "cc")

	// kill backup, then immediate Put
	fmt.Printf("Test: Put() immediately after backup failure ...\n")
	s2.Kill()
	ck.Put("a", "aaa")
	check(ck, "a", "aaa")

	for i := 0; i < sysmonitor.DeadPings*3; i++ {
		v, _ := vck.Get()
		if v.Viewnum > v1.Viewnum && v.Primary != "" && v.Backup != "" {
			break
		}
		time.Sleep(sysmonitor.PingInterval)
	}
	time.Sleep(time.Second) // wait for backup initialization
	v2, _ := vck.Get()
	if v2.Primary != s1.id || v2.Backup != s3.id {
		t.Fatal("wrong primary or backup")
	}

	check(ck, "a", "aaa")
	fmt.Printf("  ... Passed\n")

	// kill primary, then immediate Put
	fmt.Printf("Test: Put() immediately after primary failure ...\n")
	s1.Kill()
	ck.Put("b", "bbb")
	check(ck, "b", "bbb")

	for i := 0; i < sysmonitor.DeadPings*3; i++ {
		v, _ := vck.Get()
		if v.Viewnum > v2.Viewnum && v.Primary != "" {
			break
		}
		time.Sleep(sysmonitor.PingInterval)
	}
	time.Sleep(time.Second)

	check(ck, "a", "aaa")
	check(ck, "b", "bbb")
	check(ck, "c", "cc")
	fmt.Printf("  ... Passed\n")

	s1.Kill()
	s2.Kill()
	s3.Kill()
	time.Sleep(sysmonitor.PingInterval * 2)
	vs.Kill()
}

// do a bunch of concurrent Put()s on the same key,
// then check that primary and backup have identical values.
// i.e. that they processed the Put()s in the same order.
func TestConcurrentSame(t *testing.T) {
	runtime.GOMAXPROCS(4)

	tag := "cs"
	count++
	vshost := port(tag+"v", count)
	vs := sysmonitor.StartServer(vshost)
	time.Sleep(time.Second)
	vck := sysmonitor.MakeClient("", vshost)

	fmt.Printf("Test: Concurrent Put()s to the same key ...\n")

	const nservers = 2
	var sa [nservers]*KVServer
	for i := 0; i < nservers; i++ {
		sa[i] = StartKVServer(vshost, port(tag, i+1))
	}

	for iters := 0; iters < sysmonitor.DeadPings*2; iters++ {
		view, _ := vck.Get()
		if view.Primary != "" && view.Backup != "" {
			break
		}
		time.Sleep(sysmonitor.PingInterval)
	}

	// give p+b time to ack, initialize
	time.Sleep(sysmonitor.PingInterval * sysmonitor.DeadPings)

	done := false

	view1, _ := vck.Get()
	const nclients = 3
	const nkeys = 2
	for xi := 0; xi < nclients; xi++ {
		go func(i int) {
			ck := MakeKVClient(vshost)
			rr := rand.New(rand.NewSource(int64(os.Getpid() + i)))
			for done == false {
				k := strconv.Itoa(rr.Int() % nkeys)
				v := strconv.Itoa(rr.Int())
				ck.Put(k, v)
			}
		}(xi)
	}

	time.Sleep(5 * time.Second)
	done = true
	time.Sleep(time.Second)

	// read from primary
	ck := MakeKVClient(vshost)
	var vals [nkeys]string
	for i := 0; i < nkeys; i++ {
		vals[i] = ck.Get(strconv.Itoa(i))
		if vals[i] == "" {
			t.Fatalf("Get(%v) failed from primary", i)
		}
	}

	// kill the primary
	for i := 0; i < nservers; i++ {
		if view1.Primary == sa[i].id {
			sa[i].Kill()
			break
		}
	}
	for iters := 0; iters < sysmonitor.DeadPings*2; iters++ {
		view, _ := vck.Get()
		if view.Primary == view1.Backup {
			break
		}
		time.Sleep(sysmonitor.PingInterval)
	}
	view2, _ := vck.Get()
	if view2.Primary != view1.Backup {
		t.Fatal("wrong Primary")
	}

	// read from old backup
	for i := 0; i < nkeys; i++ {
		z := ck.Get(strconv.Itoa(i))
		if z != vals[i] {
			t.Fatalf("Get(%v) from backup; wanted %v, got %v", i, vals[i], z)
		}
	}

	fmt.Printf("  ... Passed\n")

	for i := 0; i < nservers; i++ {
		sa[i].Kill()
	}
	time.Sleep(time.Second)
	vs.Kill()
	time.Sleep(time.Second)
}

func TestConcurrentSameUnreliable(t *testing.T) {
	runtime.GOMAXPROCS(4)

	tag := "csu"
	count++
	vshost := port(tag+"v", count)
	vs := sysmonitor.StartServer(vshost)
	time.Sleep(time.Second)
	vck := sysmonitor.MakeClient("", vshost)

	fmt.Printf("Test: Concurrent Put()s to the same key; unreliable ...\n")

	const nservers = 2
	var sa [nservers]*KVServer
	for i := 0; i < nservers; i++ {
		sa[i] = StartKVServer(vshost, port(tag, i+1))
		sa[i].unreliable = true
	}

	for iters := 0; iters < sysmonitor.DeadPings*2; iters++ {
		view, _ := vck.Get()
		if view.Primary != "" && view.Backup != "" {
			break
		}
		time.Sleep(sysmonitor.PingInterval)
	}

	// give p+b time to ack, initialize
	time.Sleep(sysmonitor.PingInterval * sysmonitor.DeadPings)

	done := false

	view1, _ := vck.Get()
	const nclients = 3
	const nkeys = 2
	for xi := 0; xi < nclients; xi++ {
		go func(i int) {
			ck := MakeKVClient(vshost)
			rr := rand.New(rand.NewSource(int64(os.Getpid() + i)))
			for done == false {
				k := strconv.Itoa(rr.Int() % nkeys)
				v := strconv.Itoa(rr.Int())
				ck.Put(k, v)
			}
		}(xi)
	}

	time.Sleep(5 * time.Second)
	done = true
	time.Sleep(time.Second)

	// read from primary
	ck := MakeKVClient(vshost)
	var vals [nkeys]string
	for i := 0; i < nkeys; i++ {
		vals[i] = ck.Get(strconv.Itoa(i))
		if vals[i] == "" {
			t.Fatalf("Get(%v) failed from primary", i)
		}
	}

	// kill the primary
	for i := 0; i < nservers; i++ {
		if view1.Primary == sa[i].id {
			sa[i].Kill()
			break
		}
	}
	for iters := 0; iters < sysmonitor.DeadPings*2; iters++ {
		view, _ := vck.Get()
		if view.Primary == view1.Backup {
			break
		}
		time.Sleep(sysmonitor.PingInterval)
	}
	view2, _ := vck.Get()
	if view2.Primary != view1.Backup {
		t.Fatal("wrong Primary")
	}

	// read from old backup
	for i := 0; i < nkeys; i++ {
		z := ck.Get(strconv.Itoa(i))
		if z != vals[i] {
			t.Fatalf("Get(%v) from backup; wanted %v, got %v", i, vals[i], z)
		}
	}

	fmt.Printf("  ... Passed\n")

	for i := 0; i < nservers; i++ {
		sa[i].Kill()
	}
	time.Sleep(time.Second)
	vs.Kill()
	time.Sleep(time.Second)
}

// constant put/get while crashing and restarting servers
func TestRepeatedCrash(t *testing.T) {
	runtime.GOMAXPROCS(4)

	tag := "rc"
	count++
	vshost := port(tag+"v", count)
	vs := sysmonitor.StartServer(vshost)
	time.Sleep(time.Second)
	vck := sysmonitor.MakeClient("", vshost)

	fmt.Printf("Test: Repeated failures/restarts ...\n")

	const nservers = 3
	var sa [nservers]*KVServer
	for i := 0; i < nservers; i++ {
		sa[i] = StartKVServer(vshost, port(tag, i+1))
	}

	for i := 0; i < sysmonitor.DeadPings; i++ {
		v, _ := vck.Get()
		if v.Primary != "" && v.Backup != "" {
			break
		}
		time.Sleep(sysmonitor.PingInterval)
	}

	// wait a bit for primary to initialize backup
	time.Sleep(sysmonitor.DeadPings * sysmonitor.PingInterval)

	done := false

	go func() {
		// kill and restart servers
		rr := rand.New(rand.NewSource(int64(os.Getpid())))
		for done == false {
			i := rr.Int() % nservers
			// fmt.Printf("%v killing %v\n", ts(), 5001+i)
			sa[i].Kill()

			// wait long enough for new view to form, backup to be initialized
			time.Sleep(2 * sysmonitor.PingInterval * sysmonitor.DeadPings)

			sa[i] = StartKVServer(vshost, port(tag, i+1))

			// wait long enough for new view to form, backup to be initialized
			time.Sleep(2 * sysmonitor.PingInterval * sysmonitor.DeadPings)
		}
	}()

	const nth = 2
	var cha [nth]chan bool
	for xi := 0; xi < nth; xi++ {
		cha[xi] = make(chan bool)
		go func(i int) {
			ok := false
			defer func() { cha[i] <- ok }()
			ck := MakeKVClient(vshost)
			data := map[string]string{}
			rr := rand.New(rand.NewSource(int64(os.Getpid() + i)))
			for done == false {
				k := strconv.Itoa((i * 1000000) + (rr.Int() % 10))
				wanted, ok := data[k]
				if ok {
					v := ck.Get(k)
					if v != wanted {
						t.Fatalf("key=%v wanted=%v got=%v", k, wanted, v)
					}
				}
				nv := strconv.Itoa(rr.Int())
				ck.Put(k, nv)
				data[k] = nv
				// if no sleep here, then server tick() threads do not get
				// enough time to Ping the viewserver.
				time.Sleep(10 * time.Millisecond)
			}
			ok = true
		}(xi)
	}

	time.Sleep(20 * time.Second)
	done = true

	fmt.Printf("  ... Put/Gets done ... \n")

	for i := 0; i < nth; i++ {
		ok := <-cha[i]
		if ok == false {
			t.Fatal("child failed")
		}
	}

	ck := MakeKVClient(vshost)
	ck.Put("aaa", "bbb")
	if v := ck.Get("aaa"); v != "bbb" {
		t.Fatalf("final Put/Get failed")
	}

	fmt.Printf("  ... Passed\n")

	for i := 0; i < nservers; i++ {
		sa[i].Kill()
	}
	time.Sleep(time.Second)
	vs.Kill()
	time.Sleep(time.Second)
}

func TestRepeatedCrashUnreliable(t *testing.T) {
	runtime.GOMAXPROCS(4)

	tag := "rcu"
	count++
	vshost := port(tag+"v", count)
	vs := sysmonitor.StartServer(vshost)
	time.Sleep(time.Second)
	vck := sysmonitor.MakeClient("", vshost)

	fmt.Printf("Test: Repeated failures/restarts; unreliable ...\n")

	const nservers = 3
	var sa [nservers]*KVServer
	for i := 0; i < nservers; i++ {
		sa[i] = StartKVServer(vshost, port(tag, i+1))
		sa[i].unreliable = true
	}

	for i := 0; i < sysmonitor.DeadPings; i++ {
		v, _ := vck.Get()
		if v.Primary != "" && v.Backup != "" {
			break
		}
		time.Sleep(sysmonitor.PingInterval)
	}

	// wait a bit for primary to initialize backup
	time.Sleep(sysmonitor.DeadPings * sysmonitor.PingInterval)

	done := false

	go func() {
		// kill and restart servers
		rr := rand.New(rand.NewSource(int64(os.Getpid())))
		for done == false {
			i := rr.Int() % nservers
			// fmt.Printf("%v killing %v\n", ts(), 5001+i)
			sa[i].Kill()

			// wait long enough for new view to form, backup to be initialized
			time.Sleep(2 * sysmonitor.PingInterval * sysmonitor.DeadPings)

			sa[i] = StartKVServer(vshost, port(tag, i+1))

			// wait long enough for new view to form, backup to be initialized
			time.Sleep(2 * sysmonitor.PingInterval * sysmonitor.DeadPings)
		}
	}()

	const nth = 2
	var cha [nth]chan bool
	for xi := 0; xi < nth; xi++ {
		cha[xi] = make(chan bool)
		go func(i int) {
			ok := false
			defer func() { cha[i] <- ok }()
			ck := MakeKVClient(vshost)
			data := map[string]string{}
			// rr := rand.New(rand.NewSource(int64(os.Getpid()+i)))
			k := strconv.Itoa(i)
			data[k] = ""
			n := 0
			for done == false {
				v := strconv.Itoa(n)
				pv := ck.PutHash(k, v)
				if pv != data[k] {
					t.Fatalf("ck.Puthash(%s) returned %v but expected %v at iter %d\n", k, pv, data[k], n)
				}
				h := hash(data[k] + v)
				data[k] = strconv.Itoa(int(h))
				v = ck.Get(k)
				if v != data[k] {
					t.Fatalf("ck.Get(%s) returned %v but expected %v at iter %d\n", k, v, data[k], n)
				}
				// if no sleep here, then server tick() threads do not get
				// enough time to Ping the viewserver.
				time.Sleep(10 * time.Millisecond)
				n++
			}
			ok = true

		}(xi)
	}

	time.Sleep(20 * time.Second)
	done = true

	fmt.Printf("  ... Put/Gets done ... \n")

	for i := 0; i < nth; i++ {
		ok := <-cha[i]
		if ok == false {
			t.Fatal("child failed")
		}
	}

	ck := MakeKVClient(vshost)
	ck.Put("aaa", "bbb")
	if v := ck.Get("aaa"); v != "bbb" {
		t.Fatalf("final Put/Get failed")
	}

	fmt.Printf("  ... Passed\n")

	for i := 0; i < nservers; i++ {
		sa[i].Kill()
	}
	time.Sleep(time.Second)
	vs.Kill()
	time.Sleep(time.Second)
}

func proxy(t *testing.T, port string, delay *int) {
	portx := port + "x"
	os.Remove(portx)
	if os.Rename(port, portx) != nil {
		t.Fatalf("proxy rename failed")
	}
	l, err := net.Listen("unix", port)
	if err != nil {
		t.Fatalf("proxy listen failed: %v", err)
	}
	go func() {
		defer l.Close()
		defer os.Remove(portx)
		defer os.Remove(port)
		for {
			c1, err := l.Accept()
			if err != nil {
				t.Fatalf("proxy accept failed: %v\n", err)
			}
			time.Sleep(time.Duration(*delay) * time.Second)
			c2, err := net.Dial("unix", portx)
			if err != nil {
				t.Fatalf("proxy dial failed: %v\n", err)
			}

			go func() {
				for {
					buf := make([]byte, 1000)
					n, _ := c2.Read(buf)
					if n == 0 {
						break
					}
					n1, _ := c1.Write(buf[0:n])
					if n1 != n {
						break
					}
				}
			}()
			for {
				buf := make([]byte, 1000)
				n, err := c1.Read(buf)
				if err != nil && err != io.EOF {
					t.Fatalf("proxy c1.Read: %v\n", err)
				}
				if n == 0 {
					break
				}
				n1, err1 := c2.Write(buf[0:n])
				if err1 != nil || n1 != n {
					t.Fatalf("proxy c2.Write: %v\n", err1)
				}
			}

			c1.Close()
			c2.Close()
		}
	}()
}

func TestPartition1(t *testing.T) {
	runtime.GOMAXPROCS(4)

	tag := "part1"
	count++
	vshost := port(tag+"v", count)
	vs := sysmonitor.StartServer(vshost)
	time.Sleep(time.Second)
	vck := sysmonitor.MakeClient("", vshost)

	ck1 := MakeKVClient(vshost)

	fmt.Printf("Test: Old primary does not serve Gets ...\n")

	vshosta := vshost + "a"
	os.Link(vshost, vshosta)

	s1 := StartKVServer(vshosta, port(tag, 1))
	delay := 0
	proxy(t, port(tag, 1), &delay)

	deadtime := sysmonitor.PingInterval * sysmonitor.DeadPings
	time.Sleep(deadtime * 2)
	if vck.Primary() != s1.id {
		t.Fatal("primary never formed initial view")
	}

	s2 := StartKVServer(vshost, port(tag, 2))
	time.Sleep(deadtime * 2)
	v1, _ := vck.Get()
	if v1.Primary != s1.id || v1.Backup != s2.id {
		t.Fatal("backup did not join view")
	}

	ck1.Put("a", "1")
	check(ck1, "a", "1")

	os.Remove(vshosta)

	// start a client Get(), but use proxy to delay it long
	// enough that it won't reach s1 until after s1 is no
	// longer the primary.
	delay = 4
	stale_get := false
	go func() {
		x := ck1.Get("a")
		if x == "1" {
			stale_get = true
		}
	}()

	// now s1 cannot talk to viewserver, so view will change,
	// and s1 won't immediately realize.

	for iter := 0; iter < sysmonitor.DeadPings*3; iter++ {
		if vck.Primary() == s2.id {
			break
		}
		time.Sleep(sysmonitor.PingInterval)
	}
	if vck.Primary() != s2.id {
		t.Fatalf("primary never changed")
	}

	// wait long enough that s2 is guaranteed to have Pinged
	// the sysmonitor, and thus that s2 must know about
	// the new view.
	time.Sleep(2 * sysmonitor.PingInterval)

	// change the value (on s2) so it's no longer "1".
	ck2 := MakeKVClient(vshost)
	ck2.Put("a", "111")
	check(ck2, "a", "111")

	// wait for the background Get to s1 to be delivered.
	time.Sleep(5 * time.Second)
	if stale_get {
		t.Fatalf("Get to old primary succeeded and produced stale value")
	}

	check(ck2, "a", "111")

	fmt.Printf("  ... Passed\n")

	s1.Kill()
	s2.Kill()
	vs.Kill()
}

func TestPartition2(t *testing.T) {
	runtime.GOMAXPROCS(4)

	tag := "part2"
	count++
	vshost := port(tag+"v", count)
	vs := sysmonitor.StartServer(vshost)
	time.Sleep(time.Second)
	vck := sysmonitor.MakeClient("", vshost)

	ck1 := MakeKVClient(vshost)

	vshosta := vshost + "a"
	os.Link(vshost, vshosta)

	s1 := StartKVServer(vshosta, port(tag, 1))
	delay := 0
	proxy(t, port(tag, 1), &delay)

	fmt.Printf("Test: Partitioned old primary does not complete Gets ...\n")

	deadtime := sysmonitor.PingInterval * sysmonitor.DeadPings
	time.Sleep(deadtime * 2)
	if vck.Primary() != s1.id {
		t.Fatal("primary never formed initial view")
	}

	s2 := StartKVServer(vshost, port(tag, 2))
	time.Sleep(deadtime * 2)
	v1, _ := vck.Get()
	if v1.Primary != s1.id || v1.Backup != s2.id {
		t.Fatal("backup did not join view")
	}

	ck1.Put("a", "1")
	check(ck1, "a", "1")

	os.Remove(vshosta)

	// start a client Get(), but use proxy to delay it long
	// enough that it won't reach s1 until after s1 is no
	// longer the primary.
	delay = 5
	stale_get := false
	go func() {
		x := ck1.Get("a")
		if x == "1" {
			stale_get = true
		}
	}()

	// now s1 cannot talk to viewserver, so view will change.

	for iter := 0; iter < sysmonitor.DeadPings*3; iter++ {
		if vck.Primary() == s2.id {
			break
		}
		time.Sleep(sysmonitor.PingInterval)
	}
	if vck.Primary() != s2.id {
		t.Fatalf("primary never changed")
	}

	s3 := StartKVServer(vshost, port(tag, 3))
	for iter := 0; iter < sysmonitor.DeadPings*3; iter++ {
		v, _ := vck.Get()
		if v.Backup == s3.id && v.Primary == s2.id {
			break
		}
		time.Sleep(sysmonitor.PingInterval)
	}
	v2, _ := vck.Get()
	if v2.Primary != s2.id || v2.Backup != s3.id {
		t.Fatalf("new backup never joined")
	}
	time.Sleep(2 * time.Second)

	ck2 := MakeKVClient(vshost)
	ck2.Put("a", "2")
	check(ck2, "a", "2")

	s2.Kill()

	// wait for delayed get to s1 to complete.
	time.Sleep(6 * time.Second)

	if stale_get == true {
		t.Fatalf("partitioned primary replied to a Get with a stale value")
	}

	check(ck2, "a", "2")

	fmt.Printf("  ... Passed\n")

	s1.Kill()
	s2.Kill()
	s3.Kill()
	vs.Kill()
}
