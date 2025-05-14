package main

import (
	"fmt"
	"os"
	"replicatedkv/kvservice"
	"replicatedkv/sysmonitor"
	"strconv"
	"time"
)

func check(client *kvservice.KVClient, key string, value string) {
	v := client.Get(key)
	if v != value {
		fmt.Println("Error: Get(%v) -> %v, expected %v", key, v, value)
		os.Exit(-1)
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

func main() {

	tag := "example"
	monitorServerName := port(tag+"v", 1)
	monitorServer := sysmonitor.StartServer(monitorServerName)
	time.Sleep(time.Second)

	client := kvservice.MakeKVClient(monitorServerName)
	fmt.Println("Working with a single primary server first (without a backup)")
	s1 := kvservice.StartKVServer(monitorServerName, port(tag, 1))

	// wait for some time to make sure that the mointorservice will detect the new server
	// The wait time is based on how the monitor service is implemented.
	deadtime := sysmonitor.PingInterval * sysmonitor.DeadPings
	time.Sleep(deadtime * 2)

	client.Put("111", "v1")
	check(client, "111", "v1")

	client.Put("2", "v2")
	check(client, "2", "v2")

	client.Put("1", "v1a")
	check(client, "1", "v1a")

	fmt.Println("Checks passed.")

	// add a backup
	fmt.Printf("Adding a backup ...\n")
	s2 := kvservice.StartKVServer(monitorServerName, port(tag, 2))
	time.Sleep(2 * sysmonitor.PingInterval * sysmonitor.DeadPings)

	client.Put("3", "33")
	check(client, "3", "33")
	// give the backup time to initialize
	time.Sleep(3 * sysmonitor.PingInterval)

	client.Put("4", "44")
	check(client, "4", "44")

	fmt.Println("Checks passed.")

	// kill the primary

	fmt.Printf("Killing Primary ...\n")
	s1.Kill()

	fmt.Printf("See if operations will continue normally ...\n")
	check(client, "1", "v1a")
	check(client, "3", "33")
	check(client, "4", "44")

	fmt.Println("Checks passed.")

	s2.Kill()
	monitorServer.Kill()
}
