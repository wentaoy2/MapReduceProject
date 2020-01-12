package main

import (
	"net"
	"sync"
)
//globals for master node
var last_processed map[string]int
var being_processed map[string]int
var files_present map[string]int
var latest_node_with_file map[string]int

var files_present_Mutex = &sync.Mutex{}
var last_processed_Mutex = &sync.Mutex{}
var being_processed_Mutex = &sync.Mutex{}
var latest_node_with_file_Mutex = &sync.Mutex{}

var outgoing_updates chan job
var localaddress string
var OutgoingMessagesConns_Mutex = &sync.Mutex{}
var IncomingMessagesConns []net.Conn
var OutgoingMessagesConns []net.Conn

var max_replicas int
var majority int
var minute int

type job struct {
	operation  string
	filename string
	replica_node int
	serverId int
}
