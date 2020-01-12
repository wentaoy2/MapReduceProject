package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

/*
basically process arguments from user
*/
func main_init() int {
	start = time.Now()
	arguments := os.Args
	testMode = false
	sizeOfNeighbor = 3

	if len(arguments) == 3 {
		if strings.Compare(arguments[1], "-t") == 0 {
			fmt.Println("Testing mode")
			testMode = true
			return -1
		} else if strings.Compare(arguments[1], "-n") == 0 {
			fmt.Println("Normal mode")
			introducer, err := strconv.Atoi(arguments[2])
			check(err)
			return introducer
		} else {
			fmt.Println("enter -t for testing or -n # for normal failure detection mode, where # is the id of the introducer")
			return -1
		}
	} else {
		fmt.Println("INCORRECT NUMBER OF ARGUMENTS ARGUMENTS")
		fmt.Println("./Main [-n for normal or -t for testing] [introducer number]")
	}
	return -1
}

/*
find the successor of nodeid in current topology
*/
func findsuccessor(nodeid int) int {
	suc := 0
	PresentMutex.Lock()

	for i := nodeid + 1; i < numOfServer+nodeid; i++ {
		if present[i%numOfServer] == 1 {
			suc = i % numOfServer
			break
		}
	}
	PresentMutex.Unlock()
	return suc
}

/*
find predecessor of nodeid in current topology
*/
func findpredecessor(nodeid int) int {
	pred := 0
	PresentMutex.Lock()
	for i := nodeid + numOfServer - 1; i > nodeid; i-- {

		if present[i%numOfServer] == 1 {

			pred = i % numOfServer
			break
		}
	}
	PresentMutex.Unlock()
	return pred
}

/*
update the connections to neighbors
*/
func updateConnections() {
	var e error
	NeighborMutex.Lock()
	for i := 0; i < sizeOfNeighbor; i += 1 {
		if Neighbor[i] != -1 {
			if NeighborConnection[i] != nil {
				NeighborConnection[i].Close()
			}

			NeighborConnection[i], e = net.Dial("udp", fmt.Sprintf("%s:%s", Addr[Neighbor[i]], Port[Neighbor[i]]))
			check(e)
		}
	}
	NeighborMutex.Unlock()
}

/*
update the heartbeatsender array and neighbor array of current process
*/
func updateTopology() {
	aliveMutex.Lock()
	if aliveNode >= 4 {
		aliveMutex.Unlock()

		HeartbeatSenderMutex.Lock()
		HeartbeatSender[2] = findsuccessor(jsonId)
		HeartbeatSender[1] = findpredecessor(jsonId)
		HeartbeatSender[0] = findpredecessor(HeartbeatSender[1])
		HeartbeatSenderMutex.Unlock()
		NeighborMutex.Lock()

		//find two successors
		Neighbor[1] = findsuccessor(jsonId)
		Neighbor[2] = findsuccessor(Neighbor[1])
		//find one predeccessors
		Neighbor[0] = findpredecessor(jsonId)
		NeighborMutex.Unlock()
		updateConnections()
	} else if aliveNode >= 3 {
		aliveMutex.Unlock()

		//only one successor and predeccessor
		HeartbeatSenderMutex.Lock()
		HeartbeatSender[2] = findsuccessor(jsonId)
		HeartbeatSender[1] = findpredecessor(jsonId)
		HeartbeatSender[0] = -1
		HeartbeatSenderMutex.Unlock()

		NeighborMutex.Lock()
		Neighbor[2] = -1
		Neighbor[1] = findsuccessor(jsonId)
		Neighbor[0] = findpredecessor(jsonId)
		NeighborMutex.Unlock()
		updateConnections()
	} else if aliveNode > 1 {
		aliveMutex.Unlock()

		//only one predecessor
		HeartbeatSenderMutex.Lock()
		HeartbeatSender[2] = -1
		HeartbeatSender[1] = findpredecessor(jsonId)
		HeartbeatSender[0] = -1
		HeartbeatSenderMutex.Unlock()

		NeighborMutex.Lock()

		Neighbor[2] = -1
		Neighbor[1] = -1
		Neighbor[0] = findpredecessor(jsonId)

		NeighborMutex.Unlock()

		updateConnections()
	} else {
		aliveMutex.Unlock()
		//only myself in the system
		HeartbeatSenderMutex.Lock()
		HeartbeatSender[2] = -1
		HeartbeatSender[1] = -1
		HeartbeatSender[0] = -1
		HeartbeatSenderMutex.Unlock()

		NeighborMutex.Lock()

		Neighbor[2] = -1
		Neighbor[1] = -1
		Neighbor[0] = -1

		NeighborMutex.Unlock()
	}
}

/*
remove the node in membership list and check if we need to update topology
*/
func removenode(nodeid int) bool {

	//do nothing if the node is myself
	if nodeid == jsonId {
		return false
	}
	PresentMutex.Lock()
	ispresent := present[uint16(nodeid)]
	present[nodeid] = 0
	PresentMutex.Unlock()

	//do nothing if the node is already dead
	if ispresent == 0 {

		return false
	}
	aliveMutex.Lock()

	aliveNode--
	aliveMutex.Unlock()

	changeNeighbor := false

	//check if we need to update topology
	NeighborMutex.Lock()
	for i := 0; i < sizeOfNeighbor; i++ {
		if nodeid == Neighbor[i] {
			changeNeighbor = true
			MessageMutex[i].Lock()
			NeighborMessageTable[i] = make(map[int64]string)
			MessageMutex[i].Unlock()
		}

	}
	NeighborMutex.Unlock()

	HeartbeatSenderMutex.Lock()
	for i := 0; i < sizeOfNeighbor; i++ {
		if nodeid == HeartbeatSender[i] {
			changeNeighbor = true
		}
	}

	HeartbeatSenderMutex.Unlock()

	if changeNeighbor {

		updateTopology()
	}
	return (ispresent == 1)
}

/*
currently not used, can be used to check if we need to update topology when adding a node
it needs some modification and debugging.
*/
func needupdate(nodeid int) bool {
	NeighborMutex.Lock()
	if Neighbor[1] == -1 || Neighbor[2] == -1 || Neighbor[0] == -1 {
		NeighborMutex.Unlock()

		return true
	}

	if (jsonId < Neighbor[1]) && (jsonId < nodeid) && (nodeid < Neighbor[1]) {
		NeighborMutex.Unlock()

		return true

	} else if (jsonId > Neighbor[1]) && (jsonId < nodeid || nodeid < Neighbor[1]) {
		NeighborMutex.Unlock()

		return true

	} else if (Neighbor[1] < Neighbor[2]) && (Neighbor[1] < nodeid) && (nodeid < Neighbor[2]) {
		NeighborMutex.Unlock()

		return true

	} else if (Neighbor[1] > Neighbor[2]) && (Neighbor[2] > nodeid || nodeid > Neighbor[1]) {
		NeighborMutex.Unlock()

		return true

	} else if (jsonId > Neighbor[0]) && (Neighbor[0] < nodeid) && (nodeid < jsonId) {
		NeighborMutex.Unlock()

		return true
	} else if (jsonId < Neighbor[0]) && (Neighbor[0] < nodeid || nodeid < jsonId) {
		NeighborMutex.Unlock()

		return true

	} else {

		NeighborMutex.Unlock()
		return false
	}
}

/*
add the nodeid to the membership list, maybe update topology
*/
func addnode(nodeid int) bool {
	//check if the node is itself
	if nodeid == jsonId {
		return false
	}
	PresentMutex.Lock()
	//add to current global list
	ifpresent := present[nodeid]
	present[nodeid] = 1
	fmt.Println("Add node")
	PresentMutex.Unlock()

	//update topology when the node was originally dead
	if ifpresent == 0 {
		aliveMutex.Lock()

		aliveNode++
		aliveMutex.Unlock()

		fmt.Println("Update Topology")
		updateTopology()
	}
	return (ifpresent == 0)
}

/*
set up server thread and stdin
*/
func serversetup() {
	go handleUDPconnection()
	//handleStdin()
	handleConnection_client()
}

/*
set up client threads
*/
func clientsetup(introducer int) {
	if introducer == jsonId {
		//it is the first node so do nothing
		PresentMutex.Lock()
		present[jsonId] = 1
		PresentMutex.Unlock()
		aliveMutex.Lock()

		aliveNode = 1
		aliveMutex.Unlock()

		go master()
		go listenerMasterToNode()
		go listenerNodeToNode()
		var err error

		port_num, _ := strconv.Atoi(Port[Master])
		PORT_1 := strconv.Itoa(port_num + 1)
		MasterMutex.Lock()
		time.Sleep(1 * time.Second)
		MasterConnection, err = net.Dial("tcp", fmt.Sprintf("%s:%s", localaddress, PORT_1))


		MasterMutex.Unlock()
		if err != nil {
			fmt.Println("couldn't connect to incoming master thread")
			check(err)
		}

	} else {
		PresentMutex.Lock()
		present[jsonId] = 1
		PresentMutex.Unlock()
		//send join to introducer until there is a response
		buffer := make([]byte, 1024)
		introducerConn, e := net.Dial("udp", fmt.Sprintf("%s:%s", Addr[introducer], Port[introducer]))
		check(e)
		elapsed := time.Since(start)
		tempString := " " + " " + " " + " " + "JOIN " + fmt.Sprintf("%d", elapsed.Milliseconds())
		tempBytes := []byte(tempString)
		tempBytes[0] = byte(jsonId)
		var bytesRead int
		for {
			introducerConn.Write(tempBytes)
			e = introducerConn.SetReadDeadline(time.Now().Add(500 * time.Millisecond))

			check(e)
			for i := 0; i < len(buffer); i++ {
				buffer[i] = 0
			}

			bytesRead, e = introducerConn.Read(buffer)
			if err, ok := e.(net.Error); ok && err.Timeout() {
				fmt.Println("Not getting membership list")
				continue
			}
			break
		}

		fmt.Println("Membership list")

		nodes := strings.Split(string(buffer[:bytesRead]), " ")

		//the first number is the master, other numbers are in membership list
		Master, e = strconv.Atoi(nodes[0])

		//read the membership list, and update its own membership list
		for i := 1; i < len(nodes); i += 1 {
			tempNum, err := strconv.Atoi(nodes[i])
			check(err)
			println("list:")
			println(nodes[i])
			println(tempNum)
			PresentMutex.Lock()
			present[tempNum] = 1
			PresentMutex.Unlock()

		}

		aliveMutex.Lock()
		aliveNode = len(nodes)
		aliveMutex.Unlock()

		//updates neighbors
		updateTopology()

		//close connection to introducer
		introducerConn.Close()

		go listenerMasterToNode()
		go listenerNodeToNode()
		var err error
		port_num, _ := strconv.Atoi(Port[Master])
		PORT_1 := strconv.Itoa(port_num + 1)
		MasterMutex.Lock()
		MasterConnection, err = net.Dial("tcp", fmt.Sprintf("%s:%s", Addr[Master], PORT_1))
		MasterMutex.Unlock()

		check(err)
	}

	//make channels that send different messages to the message sender
	for i := 0; i < sizeOfNeighbor; i += 1 {

		NeighborChannel[i] = make(chan string)
		//open message sender threads (3)
		go UDPNeighborSender(i)
		//open heartbeat generator threads (3)
		go heartbeatGenerator(i)
	}

	//create 3 hash tables to store messages
	for i := 0; i < sizeOfNeighbor; i += 1 {
		MessageMutex[i] = &sync.Mutex{}
		NeighborMessageTable[i] = make(map[int64]string)
	}

	//create 3 heartbeat threads that timeout
	for i := 0; i < sizeOfNeighbor; i += 1 {
		heartbeatCountdown[i] = heartbeat(3*time.Second, i)
	}

	//create message sender that checks the hash tables and send messages periodically
	go broadCast()
	return
}

/*
Function that reads JSON and parse them into global variables
*/
//start
func readConfig() {
	jsonFile, err := os.Open("./Config.json")
	check(err)
	byteValue, err := ioutil.ReadAll(jsonFile)
	check(err)
	var conf Configuration
	json.Unmarshal(byteValue, &conf)
	numOfServer = conf.NumOfServer
	numOfThread = conf.NumOfThread
	NumJuiceJobs = 0
	jsonId = conf.JsonId
	deleteJuiceFiles = 0
	 sdfs_intermediate_filename_prefix = ""
	 juice_exe = ""
	 sdfs_dest_filename = ""
	 rejuicejob = ""
	 juice_start_time = 0
	 num_rejuice_completed = 0
	//Start
	//init maplereduce params
	JuiceNodes = make([]int,numOfServer)
	JuiceJobs = make([]string,numOfServer)
	//end
	Port = make([]string, numOfServer)
	Addr = make([]string, numOfServer)
	Connection = make([]net.Conn, numOfServer)
	present = make([]int, numOfServer)
	for i := 0; i < numOfServer; i++ {
		Port[i] = conf.Server[i].Port
		Addr[i] = conf.Server[i].Addr
		JuiceNodes[i] = 0
		JuiceJobs[i] = ""
		present[i] = 0
	}
	defer jsonFile.Close()
}
//end
