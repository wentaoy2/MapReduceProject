package main

import (
	"log"
	"net"
	"os"
	"sync"
	"time"
)

/*
Used for parsing JSON configuration
*/
type Configuration struct {
	JsonId      int `json:"jsonId"`
	NumOfServer int `json:"numOfServer"`
	NumOfThread int `json:"numOfThread"`
	Server      []struct {
		Addr string `json:"Addr"`
		Port string `json:"Port"`
	} `json:"Server"`
}

/*
Global variables
*/

var Connection []net.Conn
var syncStdout = log.New(os.Stdout, "", 0)
var numOfServer int
var jsonId int
var numOfThread int
var Port []string
var Addr []string
var present []int

var testMode bool

//predecessor and successor
var Neighbor [3]int = [3]int{-1, -1, -1}

//connection to neighbors
var NeighborConnection [3]net.Conn

//channel for sending messages
var NeighborChannel [3]chan string

//message hash table
var NeighborMessageTable [3]map[int64]string

//nodes that send heartbeat to us
var HeartbeatSender [3]int = [3]int{-1, -1, -1}

var HeartbeatSenderMutex = &sync.Mutex{}
var NeighborMutex = &sync.Mutex{}
var PresentMutex = &sync.Mutex{}
var MessageMutex [3]*sync.Mutex
var aliveMutex = &sync.Mutex{}

//total node
var aliveNode int = 0

//2 seconds for heart beat timeout
var heartbeatInterval int = 2

var sizeOfNeighbor int

//start time used for timestamp
var start time.Time

//heart beat that would timeout
var heartbeatCountdown [3]func(int)

//set global leave flag when user inputs leave
var LeaveMutex = &sync.Mutex{}
var leave bool = false

//log file
var file *os.File

//Start
//Juice variables
//for failure
//need to keep track of what nodes have what jobs
//this contains information about what nodes have what keys
var JuiceJobs []string
//this is the current list of active juice nodes
var JuiceNodes []int
var NumJuiceJobs int
var sdfs_intermediate_filename_prefix string
var sdfs_dest_filename string
var juice_exe string
var deleteJuiceFiles int
var JuiceNodesMutex = &sync.Mutex{}
var juice_start_time int32
var rejuicejob string
var juiceJobs_wg sync.WaitGroup
var num_rejuice_completed int
//END
