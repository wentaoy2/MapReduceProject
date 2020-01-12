package main

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"time"
)

var MasterMutex = &sync.Mutex{}
var FileMutex = &sync.Mutex{}
var Master int = 0
var elected = false
var MasterConnection net.Conn
var fileTable [10]map[string]int
var failedNode map[int]bool

//Transfer files between local directories. Used when the node uploads data to itself.
func copyFileContents(src, dst string, append bool, deleteLocal bool) (err error) {
	in, err := os.Open(src)
	check(err)

	if err != nil {
		return
	}

	//	defer in.Close()
	var out *os.File
	if append {
		out, err = os.OpenFile(dst, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	} else {
		out, err = os.OpenFile(dst, os.O_CREATE|os.O_WRONLY, 0644)
	}
	check(err)

	_, err = io.Copy(out, in)
	check(err)
	err = out.Sync()
	check(err)
	in.Close()
	out.Close()
	if deleteLocal {
		err := os.Remove(src)
		check(err)
	}
	return
}

//handle the commands sent from standard input and process them one by one
func handleConnection_client() {
	inputReader := bufio.NewReader(os.Stdin)

	start_stdin := time.Now()
	//pc, err := net.ListenPacket("udp", "127.0.0.1:4001")
	fmt.Println("entered stdin: ")

	for {
		fmt.Print("Command: ")
		currentJob, _ := inputReader.ReadString('\n')
		fmt.Println("entered command:", currentJob)
		MasterMutex.Lock()
		if underFailure {
			fmt.Println("There is no master, try again later")
			MasterMutex.Unlock()
			continue
		}
		MasterMutex.Unlock()

		currentJob = currentJob[:len(currentJob)-1]
		request := strings.Fields(currentJob)

		//handle LIST command from MP2
		if request[0] == "LIST" {
			fmt.Println("ACTIVE MACHINE IPS: ")

			PresentMutex.Lock()
			for i := 0; i < numOfServer; i++ {
				if present[i] == 1 {
					fmt.Println(Addr[i])
				}
			}
			PresentMutex.Unlock()
			//handle SELF command from MP2
		} else if request[0] == "JUICE" { //JUICE code START
			//JUICE <juice_exe> <num_juices>
			//<sdfs_intermediate_filename_prefix> <sdfs_dest_filename>
			//delete_input={0,1}
			//sending master the juice command
			fmt.Println("Entered Juice command ")
			juice_msg := "MAS JUICE " + request[1] + " " + request[2] + " " + request[3] + " " + request[4] + " " + request[5] + "\n"
			fmt.Fprintf(MasterConnection, juice_msg)
			//END
		} else if request[0] == "SELF" {
			elapsed := time.Since(start_stdin)

			fmt.Println("machine time: ", elapsed)
			fmt.Println("machine id: ", jsonId)
			fmt.Println("machine ip: ", Addr[jsonId])
			//handle LEAVE command from MP2
		} else if request[0] == "LEAVE" {

			fmt.Println("NODE is VOLUNTARILY LEAVING")
			LeaveMutex.Lock()
			leave = true
			LeaveMutex.Unlock()
			break
			//handle GET command from, send request to master to get file
		} else if request[0] == "GET" {
			fileGetter(request[1], request[2])
		} else if request[0] == "PUT" {
			filename := request[1]
			localFileName := request[2]
			fmt.Println("local and global filenames:", localFileName, filename)
			fmt.Fprintf(MasterConnection, "MAS"+" "+"PUT"+" "+filename+"\n")
			reader := bufio.NewReader(MasterConnection)

			netData, err := reader.ReadString('\n')
			if err != nil {
				fmt.Println("MasterConnection Failed!\n")
				break
			}
			netData = netData[:len(netData)-1]
			nodeSize, err := strconv.Atoi(netData)

			//process confirm if sending files with same filename too fast
			if err != nil && netData == "CONFIRM" {
				fmt.Println("THERE MAY BE A CONFLICT, CONFIRM THE PUT? Y/N")
				userResponse, _ := inputReader.ReadString('\n')
				userResponse = userResponse[:len(userResponse)-1]
				for userResponse != "N" && userResponse != "Y" {
					fmt.Println("Y/N!")
					userResponse, _ = inputReader.ReadString('\n')
					userResponse = userResponse[:len(userResponse)-1]
				}
				if userResponse == "N" {
					fmt.Fprintf(MasterConnection, "N\n")
					continue
				} else {
					fmt.Fprintf(MasterConnection, "Y\n")
					netData, err := reader.ReadString('\n')
					if err != nil {
						fmt.Println("MasterConnection Failed!\n")
						continue
					}
					//check(err)
					netData = netData[:len(netData)-1]
					nodeSize, err = strconv.Atoi(netData)
					check(err)
				}
			}
			fmt.Println("nodesize:", nodeSize)

			var listOfNode []int
			for i := 0; i < nodeSize; i++ {
				netData, err := reader.ReadString('\n')
				if err != nil {
					fmt.Println("MasterConnection Failed!\n")
					break
				}
				fmt.Println("PUT to node " + netData)

				netData = netData[:len(netData)-1]
				nodeID, err := strconv.Atoi(netData)
				check(err)
				listOfNode = append(listOfNode, nodeID)
			}
			now := time.Now() // current local time
			sec := now.UnixNano()
			fmt.Println("Current date and time is: ", sec)
			for i := 0; i < nodeSize; i++ {
				if listOfNode[i] == jsonId {
					err := copyFileContents(localFileName, "storage/"+filename, false, false)
					check(err)
				} else {
					go fileSender(listOfNode[i], localFileName, "storage/"+filename, false, false)
				}
			}
			sec = now.UnixNano()
			fmt.Println("Current date and time is: ", sec)

			//handle ls command, basically list all machines with the file
		} else if request[0] == "LS" {
			filename := request[1]
			PresentMutex.Lock()
			FileMutex.Lock()
			for i := 0; i < len(present); i++ {
				if present[i] == 1 {
					if _, ok := fileTable[i][filename]; ok {
						fmt.Println("Node " + strconv.Itoa(i) + ": " + Addr[i])
					}
				}
			}
			FileMutex.Unlock()
			PresentMutex.Unlock()

			//Handle DELETE command, basically asks master to delete all files
		} else if request[0] == "DELETE" {
			filename := request[1]
			fmt.Fprintf(MasterConnection, "MAS"+" "+"DELETE"+" "+filename+"\n")

			//Handle STORE command, basically checks local storage
		} else if request[0] == "STORE" {
			FileMutex.Lock()
			for k := range fileTable[jsonId] {
				fmt.Println("Files stored in current node are:")
				fmt.Println(k)
			}
			FileMutex.Unlock()
		} else if request[0] == "maple" {
			mapperExeName := request[1]
			numOfMapper, _ := strconv.Atoi(request[2])
			intermediateFileName := request[3]
			inputFileDir := request[4]
			aliveMutex.Lock()
			if numOfMapper > aliveNode {
				fmt.Println("Maple more than number of nodes\n")
				numOfMapper = aliveNode
			}
			aliveMutex.Unlock()
			fmt.Fprintf(MasterConnection, "MR"+" "+"MAPLE"+" "+mapperExeName+" "+strconv.Itoa(numOfMapper)+" "+intermediateFileName+" "+inputFileDir+"\n")
		}

	}
}

func copyFileContentsWG(src, dst string, append bool, deleteLocal bool, wg *sync.WaitGroup) (err error) {
	in, err := os.Open(src)
	check(err)
	if err != nil {
		return
	}
	//check(err)
	/*
		if err != nil {

			return
		}
	*/

	//wg.Done()

	var out *os.File
	if append {
		out, err = os.OpenFile(dst, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	} else {
		out, err = os.OpenFile(dst, os.O_CREATE|os.O_WRONLY, 0644)
	}
	check(err)
	_, err = io.Copy(out, in)
	check(err)
	err = out.Sync()
	check(err)
	in.Close()
	cerr := out.Close()
	check(cerr)
	if deleteLocal {
		os.Remove(src)
		//err := os.Remove(src)
		//check(err)
	}
	wg.Done()
	return
}

func fileSenderWG(nodeNum int, localName string, globalName string, append bool, deleteLocal bool, wg *sync.WaitGroup) {
	f, err := os.Open(localName)
	check(err)
	if err != nil {
		return
	}
	fileInfo, err := f.Stat()
	//check(err)
	if err != nil {
		return
	}

	var c net.Conn
	c, err = net.Dial("tcp", fmt.Sprintf("%s:%s", Addr[nodeNum], Port[nodeNum]))
	if err != nil {
		return
	}
	//check(err)
	fmt.Fprintf(c, "PUT"+" "+globalName+" "+strconv.FormatBool(append)+"\n")
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], uint64(fileInfo.Size()))
	//fmt.Println("sending size", uint64(fileInfo.Size()))
	_, err = c.Write(buf[:])
	if err != nil {
		return
	}
	//check(err)
	transferReader := bufio.NewReader(c)
	//fmt.Fprintf(c, strconv.Itoa(fileInfo.Size())+"\n")
	acknowledge, err := transferReader.ReadString('\n')
	if err != nil {
		fmt.Println("TempConnection Failed!\n")
		return
	}
	//check(err)
	acknowledge = acknowledge[:len(acknowledge)-1]
	if acknowledge == "ACK" {
		counter := 0
		buffer := make([]byte, 1024*1024)
		for counter < int(fileInfo.Size()) {
			n, err := f.Read(buffer)
			check(err)
			fmt.Fprintf(c, string(buffer[:n]))
			counter += n
		}
		//	fmt.Println("done sending file ", globalName)
		//		now := time.Now() // current local time
		//		sec := now.UnixNano()
		//		fmt.Println("Current date and time is: ", sec)
	} else {
		fmt.Println("Not getting ACK!")
	}
	c.Close()
	f.Close()
	if deleteLocal {
		os.Remove(localName)
		//err := os.Remove(localName)
		//check(err)
	}
	wg.Done()
}

func fileGetter(filename string, localFileName string) bool {
	//filename := request[1]
	fmt.Fprintf(MasterConnection, "MAS"+" "+"GET"+" "+filename+"\n")
	reader := bufio.NewReader(MasterConnection)
	netData, err := reader.ReadString('\n')
	if err != nil {
		fmt.Println("MasterConnection Failed!\n")
		//continue
		return false
	}
	//check(err)
	netData = netData[:len(netData)-1]
	nodeID, err := strconv.Atoi(netData)
	check(err)
	//localFileName := request[2]

	file, err := os.Create(localFileName)
	check(err)
	file.Close()
	//}
	//f, err := os.OpenFile(localFileName, os.O_APPEND|os.O_WRONLY, os.ModePerm)
	f, err := os.OpenFile(localFileName, os.O_WRONLY, os.ModePerm)
	check(err)

	//decide if we

	//just get file from local directory
	if nodeID == jsonId {
		now := time.Now() // current local time
		sec := now.UnixNano()
		fmt.Println("Current date and time for curr machine: ", sec)
		err := copyFileContents("storage/"+filename, localFileName, false, false)
		sec = now.UnixNano()
		fmt.Println("Current date and time for curr machine: ", sec)
		check(err)
		f.Close()
		return true
	} else {

		temperaryConnection, err := net.Dial("tcp", fmt.Sprintf("%s:%s", Addr[nodeID], Port[nodeID]))
		//	}
		if err != nil {
			fmt.Println("Dial Failed!\n")
			//continue
			return false
		}
		fmt.Fprintf(temperaryConnection, "GET"+" "+filename+"\n")
		reader2 := bufio.NewReader(temperaryConnection)
		var sizeBuf [8]byte
		_, err = io.ReadFull(reader2, sizeBuf[:])
		if err != nil {
			fmt.Println("TempConnection Failed!\n")
			//continue
			return false
		}
		size := int(binary.BigEndian.Uint64(sizeBuf[:]))
		fmt.Fprintf(temperaryConnection, "ACK\n")
		now := time.Now() // current local time
		sec := now.UnixNano()
		fmt.Println("Current date and time for curr machine: ", sec)
		buf := make([]byte, 1024*1024)
		counter := 0
		for counter < size {
			n, err := reader2.Read(buf)
			if err != nil {
				fmt.Println("TempConnection Failed!\n")
				break
			}
			//check(err)
			f.Write(buf[:n])
			counter += n
		}

		err = f.Sync()
		sec = now.UnixNano()
		fmt.Println("Current date and time for curr machine: ", sec)
		check(err)
		f.Close()

		temperaryConnection.Close()
		return true
	}
}

//helper function that establish a temperary connection and transfer file
func fileSender(nodeNum int, localName string, globalName string, append bool, deleteLocal bool) {
	f, err := os.Open(localName)
	check(err)
	fileInfo, err := f.Stat()
	check(err)

	var c net.Conn
	c, err = net.Dial("tcp", fmt.Sprintf("%s:%s", Addr[nodeNum], Port[nodeNum]))
	if err != nil {
		return
	}
	//check(err)
	fmt.Fprintf(c, "PUT"+" "+globalName+" "+strconv.FormatBool(append)+"\n")
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], uint64(fileInfo.Size()))
	//fmt.Println("sending size", uint64(fileInfo.Size()))
	_, err = c.Write(buf[:])
	//check(err)
	if err != nil {
		return
	}
	transferReader := bufio.NewReader(c)
	//fmt.Fprintf(c, strconv.Itoa(fileInfo.Size())+"\n")
	acknowledge, err := transferReader.ReadString('\n')
	if err != nil {
		fmt.Println("TempConnection Failed!\n")
		return
	}
	//check(err)
	acknowledge = acknowledge[:len(acknowledge)-1]
	if acknowledge == "ACK" {
		counter := 0
		buffer := make([]byte, 1024*1024)
		for counter < int(fileInfo.Size()) {
			n, err := f.Read(buffer)
			check(err)
			fmt.Fprintf(c, string(buffer[:n]))
			counter += n
		}
		//	fmt.Println("done sending file ", globalName)
		//now := time.Now() // current local time
		//sec := now.UnixNano()
		//	fmt.Println("Current date and time is: ", sec)
	} else {
		fmt.Println("Not getting ACK!")
	}
	c.Close()
	f.Close()
	if deleteLocal {
		//err := os.Remove(localName)
		//check(err)
		os.Remove(localName)
	}
}

//handle connections sent from other nodes
func handleConnection_server(c net.Conn) {
	//fmt.Printf("Serving %s\n", c.RemoteAddr().String())

	reader := bufio.NewReader(c)
	//defer c.Close()

	netData, err := reader.ReadString('\n')
	if err != nil {
		//check(err)
		fmt.Println("Failing Connection from server!")
		c.Close()
		return
	}
	//check(err)
	fmt.Println("Node to node M: " + netData)
	netData = netData[:len(netData)-1]
	request := strings.Fields(netData)

	//handle election message in the ring elecion protocol
	if request[0] == "ELECTION" {
		//MasterMutex.Lock()
		var electionConnection net.Conn
		for {
			suc := findsuccessor(jsonId)
			electionConnection, err = net.Dial("tcp", fmt.Sprintf("%s:%s", Addr[suc], Port[suc]))
			if err == nil {
				break
			}
		}

		candidate, e := strconv.Atoi(request[1])
		check(e)
		//It is elected to be the master
		if candidate == jsonId {
			fmt.Fprintf(electionConnection, "ELECTED"+" "+strconv.Itoa(jsonId)+"\n")
			elected = false
			Master = jsonId
			go master()
			var err error

			port_num, _ := strconv.Atoi(Port[Master])
			PORT_1 := strconv.Itoa(port_num + 1)

			time.Sleep(1 * time.Second)
			MasterConnection, err = net.Dial("tcp", fmt.Sprintf("%s:%s", localaddress, PORT_1))
			if err != nil {
				fmt.Println("couldn't connect to incoming master thread")
				check(err)
			}

		} else if candidate > jsonId {
			fmt.Fprintf(electionConnection, "ELECTION"+" "+strconv.Itoa(candidate)+"\n")
			elected = true
		} else {
			if elected == false {
				fmt.Fprintf(electionConnection, "ELECTION"+" "+strconv.Itoa(jsonId)+"\n")
				elected = true
			}
		}

		electionConnection.Close()
		//MasterMutex.Unlock()

		//handle elected message in the ring elecion protocol
	} else if request[0] == "ELECTED" {

		var electionConnection net.Conn
		var err error
		var suc int
		for {
			suc = findsuccessor(jsonId)
			electionConnection, err = net.Dial("tcp", fmt.Sprintf("%s:%s", Addr[suc], Port[suc]))
			if err == nil {
				break
			}
		}
		candidate, e := strconv.Atoi(request[1])
		check(e)
		MasterMutex.Lock()
		Master = candidate
		MasterMutex.Unlock()

		if Master == jsonId {
			fmt.Println("Elected goes back.")
			go master()

			port_num, _ := strconv.Atoi(Port[Master])
			PORT_1 := strconv.Itoa(port_num + 1)
			time.Sleep(1 * time.Second)
			MasterConnection, err = net.Dial("tcp", fmt.Sprintf("%s:%s", "127.0.0.1", PORT_1))

		} else {
			port_num, _ := strconv.Atoi(Port[Master])
			PORT_1 := strconv.Itoa(port_num + 1)
			time.Sleep(2 * time.Second)
			MasterConnection, err = net.Dial("tcp", fmt.Sprintf("%s:%s", Addr[Master], PORT_1))
		}

		if candidate != suc {
			fmt.Fprintf(electionConnection, "ELECTED"+" "+strconv.Itoa(candidate)+"\n")
		}
		elected = false

		electionConnection.Close()

		//Other nodes want to get file
	} else if request[0] == "GET" {
		filename := "storage/" + request[1]
		f, err := os.Open(filename)
		check(err)
		fileInfo, err := f.Stat()
		check(err)

		var buf [8]byte
		binary.BigEndian.PutUint64(buf[:], uint64(fileInfo.Size()))
		_, err = c.Write(buf[:])
		now := time.Now() // current local time
		sec := now.UnixNano()
		fmt.Println("Current date and time for curr machine for GET: ", sec)

		acknowledge, err := reader.ReadString('\n')
		if err != nil {
			fmt.Println("TempConnection Failed GET!\n")
		}
		//check(err)
		acknowledge = acknowledge[:len(acknowledge)-1]
		if acknowledge == "ACK" {
			counter := 0
			buffer := make([]byte, 1024*1024)
			for counter < int(fileInfo.Size()) {
				n, err := f.Read(buffer)
				check(err)
				//fmt.Fprintf(c, string(buffer[:n]))
				c.Write(buffer[:n])
				counter += n
			}
			now := time.Now() // current local time
			sec = now.UnixNano()
			fmt.Println("Current date and time for curr machine for GET: ", sec)

		} else {
			fmt.Println("Not getting ACK!")
		}
		f.Close()

		//Other nodes want to put file
	} else if request[0] == "PUT" {
		filename := request[1]
		//filename = "storage/" + filename

		//var file, err = os.Create(filename)
		append, _ := strconv.ParseBool(request[2])
		//check(err)
		//file.Close()

		var f *os.File

		if append {
			f, err = os.OpenFile(filename, os.O_CREATE|os.O_APPEND|os.O_WRONLY, os.ModePerm)
		} else {
			f, err = os.OpenFile(filename, os.O_CREATE|os.O_WRONLY, os.ModePerm)
		}

		check(err)
		var sizeBuf [8]byte
		//fmt.Println("GETTING SIZE")
		_, err = io.ReadFull(reader, sizeBuf[:])
		if err != nil {
			fmt.Println("TempConnection Failed!\n")
		}

		size := int(binary.BigEndian.Uint64(sizeBuf[:]))
		fmt.Fprintf(c, "ACK\n")
		buf := make([]byte, 1024*1024)
		counter := 0
		//	fmt.Println("received size", size)
		for counter < size {

			n, err := reader.Read(buf)

			//check(err)
			if err != nil {
				break
			}
			_, err = f.Write(buf[:n])
			check(err)
			counter += n
		}

		err = f.Sync()

		check(err)
		f.Close()

		//check if received file is part of a juice job
		//and perform juice job if so
		if IsJuiceJob(filename[8:len(filename)]) {
			performJuiceJob(filename)
		}
	}

	c.Close()
}

//handle connection sent from master node
func handleConnection_fromMaster(c net.Conn) {
	//	fmt.Printf("Serving %s\n", c.RemoteAddr().String())
	fmt.Println("Connection from master to node")
	reader := bufio.NewReader(c)
	defer c.Close()
	for {

		netData, err := reader.ReadString('\n')
		if err != nil {
			fmt.Println("Failing Connection from master!", err)
			//c.Close()
			return
		}

		netData = netData[:len(netData)-1]
		request := strings.Fields(netData)
		//fmt.Println("request", request)

		//master wants to update the local file table
		if request[0] == "UPDATE" {
			//fmt.Println("UPDATING")
			nodeID, err := strconv.Atoi(request[2])
			check(err)
			filename := request[3]
			//master wants to delete the local file table
			if request[1] == "DELETE" {
				//fmt.Println("UPDATING DELETE")

				FileMutex.Lock()
				delete(fileTable[nodeID], filename)
				FileMutex.Unlock()
				//master wants to add to the local file table
			} else if request[1] == "PUT" {
				//fmt.Println("UPDATING PUT")

				FileMutex.Lock()
				if fileTable[nodeID] == nil {
					fileTable[nodeID] = map[string]int{}
				}
				fileTable[nodeID][filename] = 1
				FileMutex.Unlock()
			}

			//master wants the node to delte the local file
		} else if request[0] == "DELETE" {
			filename := request[1]
			err := os.Remove("storage/" + filename)
			check(err)
			//master wants the local to transfer file to another node
		} else if request[0] == "JUICE" { //START
			//command received
			//JUICE [filename prefix] [reduce_exe] [sdfs_dest_filename] [DELETE = {0,1}] [list of keys]
			//process each one individually and save results to local intermediate file
			reducer_intermediate_filename := "reducer_intermediate_file" + strconv.Itoa(jsonId) + ".txt"
			f, _ := os.Create(reducer_intermediate_filename)
			f.Close()
			f, _ = os.OpenFile(reducer_intermediate_filename, os.O_APPEND|os.O_WRONLY, os.ModePerm)
			//assumes [reduce_exe] has been built already
			deleteJuiceFiles, _ = strconv.Atoi(request[4])
			juice_exe = request[2]
			sdfs_intermediate_filename_prefix = request[1]
			sdfs_dest_filename = request[3]

			for i := 5; i < len(request); i++ {
				wg_id := i - 5
				//	fmt.Println("starting worker thread: ", wg_id)
				//	juiceJobs_wg.Add(1)
				juice_job_worker(wg_id, request[i], sdfs_intermediate_filename_prefix, f) //&juiceJobs_wg,wg_id,

				//todo: put in go routine?
				//do i have to worry about concurent writes to the intermediate file?
				//wait groups here https://goinbigdata.com/golang-wait-for-all-goroutines-to-finish/
			}
			//PUT reducer_intermediate_filename.txt to sdfs [sdfs_dest_filename]
			//send MAS JUICEFIN
			//fmt.Println("Waiting for workers to finish")
			//juiceJobs_wg.Wait()
			//	fmt.Println("all workers done")

			//todo
			//send intermediate file and delete it
			f.Close()
			fmt.Println("Sending JUICEFIN [jsonid]")
			juice_msg := "MAS JUICEFIN " + strconv.Itoa(jsonId) + "\n"
			fileSender(Master, reducer_intermediate_filename, "storage/"+reducer_intermediate_filename, false, false)
			fmt.Fprintf(MasterConnection, juice_msg)
			//master sent a PUT request to a node
		} else if request[0] == "REJUICE" {

			//todo
			//could make it faster by spawnign threads from each file
			//JUICE [filename prefix] [reduce_exe] [sdfs_dest_filename] [DELETE = {0,1}] [list of keys]
			reducer_intermediate_filename := "reducer_intermediate_file_rejuice" + strconv.Itoa(jsonId) + ".txt"
			f, _ := os.Create(reducer_intermediate_filename)
			f.Close()

			f, _ = os.OpenFile(reducer_intermediate_filename, os.O_APPEND|os.O_WRONLY, os.ModePerm)
			defer f.Close()
			//assumes [reduce_exe] has been built already
			file_name := ""
			deleteJuiceFiles, _ = strconv.Atoi(request[4])
			juice_exe = request[2]
			sdfs_intermediate_filename_prefix = request[1]
			sdfs_dest_filename = request[3]
			onlylocal := 0
			fmt.Println("************** JUICE FIN")

			FileMutex.Lock()
			for k := range fileTable[jsonId] {
				fmt.Println("Files stored in current node are:")
				fmt.Println(k)
			}
			FileMutex.Unlock()
			fmt.Println("************** JUICE FIN")
			needGET := true
			for i := 5; i < len(request); i++ {

				fmt.Println("got key files", request[i]) //)+j], "of size ",request[i+j+1])
				//process the request and save it to local intermediate file
				//pass in [file_name] to [reduce_exe]
				key := request[i][len(request[1])+1 : len(request[i])-4]
				fmt.Println("app: ", juice_exe, " filename ", file_name)

				//get the file from sdfs if you dont have it
				if fileExists("storage/" + request[i]) {
					fmt.Println("sdfs stroes file locally.  Using that file for reduce")
					file_name = "storage/" + request[i]
					rejuicejob = rejuicejob + request[i] + " "

				} else if fileExists(request[i]) {
					rejuicejob = rejuicejob + request[i] + " "

					onlylocal = 1
					file_name = request[i]

				} else {
					FileMutex.Lock()
					for k := range fileTable[jsonId] {
						if k == request[i] {
							needGET = false
						}
					}
					FileMutex.Unlock()
					if needGET {
						fmt.Println("getting file that isn't going to be replicated here by a PUT")
						file_name = request[i]
						rejuicejob = rejuicejob + request[i] + " "
						//todo
						//problem with not receiving a PUT update but getting a vm num for itself when it doesn't have the file
						JuiceGET(request[i])
					} else {
						rejuicejob = rejuicejob + request[i] + " "
						needGET = true
						continue
					}
				}
				out, err := exec.Command(juice_exe, file_name).Output()
				if err != nil {
					log.Fatal(err)
				}
				fmt.Println("REDUCE OUTPUT ", string(out), "for key ", key)
				//append to intermediate file
				f.Write([]byte(key))
				f.Write([]byte(" "))
				f.Write((out))
				f.Write([]byte("\n"))
				//delete the sdfs files if necessary
				if deleteJuiceFiles == 1 && onlylocal == 1 {
					err := os.Remove(file_name)
					check(err)
				}
				num_rejuice_completed = num_rejuice_completed + 1

			}
			//PUT reducer_intermediate_filename.txt to sdfs [sdfs_dest_filename]
			//send MAS JUICEFIN
			//todo send mas juice fin at right time
			//todo do i need a mutex on num_rejuice_completed?
			fmt.Println("num of compelted rejuice jobs", num_rejuice_completed)
			fmt.Println("rejuice jobs: ", rejuicejob)
			juiceFiles := strings.Fields(rejuicejob)
			if num_rejuice_completed == len(juiceFiles) {
				fmt.Println("Sending JUICEFIN [jsonid] from REJUICE")
				juice_msg := "MAS JUICEFIN " + strconv.Itoa(jsonId) + "\n"
				fmt.Fprintf(MasterConnection, juice_msg)
				rejuicejob = ""
				num_rejuice_completed = 0
				fileSender(Master, reducer_intermediate_filename, "storage/"+reducer_intermediate_filename, false, false)

			}
			//END
		} else if request[0] == "PUT" {
			filename := request[2]
			nodeID, err := strconv.Atoi(request[1])
			check(err)

			go fileSender(nodeID, "storage/"+filename, "storage/"+filename, false, false)
		} else if request[0]+" "+request[1] == "MR MAPLE" {
			fmt.Println("Start maple")
			mapperExeName := request[2]
			intermediateFileName := request[3]
			//inputFileDir := request[4]

			//reader := bufio.NewReader(MasterConnection)
			netData, err := reader.ReadString('\n')
			check(err)
			netData = netData[:len(netData)-1]
			fileSize, err := strconv.Atoi(netData)

			fmt.Println("get filesize", fileSize)
			var filesInDirectory []string
			for i := 0; i < fileSize; i++ {
				netData, err := reader.ReadString('\n')
				check(err)
				netData = netData[:len(netData)-1]
				filesInDirectory = append(filesInDirectory, netData)
				//nodeSize, err := strconv.Atoi(netData)
			}

			fmt.Println("Finish get file names", fileSize)
			for i := 0; i < fileSize; i++ {
				for {
					getResult := fileGetter(filesInDirectory[i], filesInDirectory[i])
					if getResult == true {
						break
					}
				}
			}

			fileMap := map[string]string{}
			//finalOut := ""
			for i := 0; i < fileSize; i++ {

				//tempName := fi.Name()
				out, err := exec.Command(mapperExeName, filesInDirectory[i]).Output()
				check(err)
				//reader := bufio.NewReader(out)
				stringOut := string(out)

				outArray := strings.Split(stringOut, "\n")
				fmt.Println("size of words", len(outArray))
				index := 0
				for {

					if index == len(outArray)-1 {
						break
					}

					text := outArray[index]
					entry := strings.Fields(text)

					tempFileName := intermediateFileName + "_" + entry[0]

					if _, ok := fileMap[tempFileName]; ok {
						fileMap[tempFileName] += entry[1] + "\n"
					} else {
						fileMap[tempFileName] = entry[1] + "\n"
					}

					index += 1
				}

			}

			fmt.Println("Node finish local", len(fileMap))

			tempC := MasterConnection
			check(err)
			numOfFile := strconv.Itoa(len(fileMap))
			machineId := strconv.Itoa(jsonId)
			fmt.Fprintf(tempC, "MR"+" "+"TRANSFER"+" "+numOfFile+" "+machineId+"\n")

			for localFileName, val := range fileMap {
				fmt.Fprintf(tempC, localFileName+"\n")
				var buf [8]byte

				binary.BigEndian.PutUint64(buf[:], uint64(len(val)))
				//fmt.Println("sending size", uint64(fileInfo.Size()))
				_, err = tempC.Write(buf[:])
				check(err)
				transferReader := bufio.NewReader(tempC)
				//fmt.Fprintf(c, strconv.Itoa(fileInfo.Size())+"\n")
				acknowledge, err := transferReader.ReadString('\n')
				if err != nil {
					fmt.Println("TempConnection Failed!\n")
					return
				}
				//check(err)
				acknowledge = acknowledge[:len(acknowledge)-1]
				if acknowledge == "ACK" {
					fmt.Fprintf(c, val)

					counter := 0
					for counter < len(val) {
						add := 256
						if add > len(val)-counter {
							add = len(val) - counter
						}
						fmt.Fprintf(tempC, val[counter:counter+add])
						counter += add
					}

				} else {
					fmt.Println("Not getting ACK!")
				}
			}
			//c.Close()

			fmt.Println("MR DONE")

		}
	}

}

//listen to connection from master and let it handled by a thread
func listenerMasterToNode() {
	port, _ := strconv.Atoi(Port[jsonId])
	M2Nport := strconv.Itoa(port + 2)
	PORT := ":" + M2Nport
	l, err := net.Listen("tcp", PORT)
	if err != nil {
		fmt.Println(err)
		return
	}
	defer l.Close()
	rand.Seed(time.Now().Unix())
	fmt.Println("Node starts to listen to master")
	for {

		c, err := l.Accept()
		if err != nil {
			fmt.Println(err)
			return
		}

		go handleConnection_fromMaster(c)
	}
}

//listen to connection from another node and let it handled by a thread
func listenerNodeToNode() {
	l, err := net.Listen("tcp", ":"+Port[jsonId])
	if err != nil {
		fmt.Println(err)
		return
	}
	rand.Seed(time.Now().Unix())
	fmt.Println("Listen to node to node connection")
	defer l.Close()
	for {

		c, err := l.Accept()
		if err != nil {
			check(err)
			fmt.Println(err)
			return
		}
		go handleConnection_server(c)
	}
}

//Start the election protocol
func callElection() {
	//MasterMutex.Lock()
	aliveMutex.Lock()
	if aliveNode == 1 {
		aliveMutex.Unlock()
		Master = jsonId
		go master()
		var err error

		port_num, _ := strconv.Atoi(Port[Master])
		PORT_1 := strconv.Itoa(port_num + 1)

		time.Sleep(1 * time.Second)
		MasterConnection, err = net.Dial("tcp", fmt.Sprintf("%s:%s", localaddress, PORT_1))
		if err != nil {
			fmt.Println("couldn't connect to incoming master thread")
			check(err)
		}
	} else {
		aliveMutex.Unlock()
		if elected == false {
			var electionConnection net.Conn
			var err error
			for {
				suc := findsuccessor(jsonId)
				electionConnection, err = net.Dial("tcp", fmt.Sprintf("%s:%s", Addr[suc], Port[suc]))
				if err == nil {
					break
				}
			}
			fmt.Fprintf(electionConnection, "ELECTION"+" "+strconv.Itoa(jsonId)+"\n")
			elected = true
			electionConnection.Close()
		}
	}
}

var underFailure = false

//failure handling procedure, calls election and starts replication here
func handleFailure(nodeID int) {
	fmt.Println("Detect failure: ", nodeID)
	MasterMutex.Lock()

	if underFailure == false {
		underFailure = true
		failedNode[nodeID] = true
		MasterMutex.Unlock()
	} else {
		failedNode[nodeID] = true
		MasterMutex.Unlock()
		return
	}

	//wait for membership to stabilize?
	time.Sleep(5 * time.Second)

	for i := 0; i < 10; i++ {
		if failedNode[i] == true && i == Master {
			fmt.Println("Calling Election")
			callElection()
		}
	}

	if jsonId == Master {
		leaveid := nodeID
		fmt.Println("Restore Data")
		restoreData(outgoing_updates)
		time.Sleep(1 * time.Second)

		//start
		aliveMutex.Lock()
		num_alive_nodes := aliveNode
		aliveMutex.Unlock()
		fmt.Println(leaveid, " left")
		//todo
		//rejuices are only sent to one node which can be bad split into multiple if possible

		//// TODO:
		//also rejuice doesn't work if the node that does the rejuice doesn't received the PUT files (there are more than 4 nodes that exist)
		//that were on the failed nodes
		//where to put the get of the files that aren't put
		//need to concatenate all jobs from all failed nodes
		//also change the message type
		cum_job_msg := ""
		index := leaveid
		nodeDoesntHaveJob := 0
		JuiceNodesMutex.Lock()
		if JuiceNodes[leaveid] == 1 {
			//find out if a node doesn't have a job and has been selected as juice node
			for i := 0; i < numOfServer; i++ {
				PresentMutex.Lock()
				if present[i] == 1 && JuiceNodes[i] == 0 && JuiceJobs[i] == "" && leaveid != i {
					nodeDoesntHaveJob = 1
					index = i
				}
				PresentMutex.Unlock()
			}
			for i := 0; i < numOfServer; i++ {
				PresentMutex.Lock()
				if present[i] == 1 {
					fmt.Println(i, " is present")
				}
				PresentMutex.Unlock()
			}
			PresentMutex.Lock()
			for i := 0; i < numOfServer; i++ {
				if JuiceJobs[i] != "" && present[i] == 0 {
					cum_job_msg = cum_job_msg + JuiceJobs[i] + " "
				}
			}
			PresentMutex.Unlock()

			JuiceNodes[leaveid] = 0
			fmt.Println("resend job: ", cum_job_msg)
			//if a node doesn't have a job send to it first
			//otherwise send to first successor
			if nodeDoesntHaveJob == 1 {
				PresentMutex.Lock()
				if present[index] == 1 {
					fmt.Println("sending to since it doesn't have a job ", index)
					outgoing_sender_con := OutgoingMessagesConns[index]
					JuiceNodes[index] = 1
					JuiceJobs[index] = JuiceJobs[index] + cum_job_msg
					juice_msg := "REJUICE " + sdfs_intermediate_filename_prefix + " " + juice_exe + " " + sdfs_dest_filename + " " + strconv.Itoa(deleteJuiceFiles) + " " + cum_job_msg + "\n"
					fmt.Fprintf(outgoing_sender_con, juice_msg)
				}
				PresentMutex.Unlock()
			} else if NumJuiceJobs > num_alive_nodes {
				index = leaveid
				for i := 0; i < numOfServer; i++ {
					index = (index + 1) % numOfServer
					PresentMutex.Lock()
					if present[index] == 1 {
						fmt.Println("sending to ", index)
						outgoing_sender_con := OutgoingMessagesConns[index]
						JuiceNodes[index] = 1
						JuiceJobs[index] = JuiceJobs[index] + cum_job_msg
						juice_msg := "REJUICE " + sdfs_intermediate_filename_prefix + " " + juice_exe + " " + sdfs_dest_filename + " " + strconv.Itoa(deleteJuiceFiles) + " " + cum_job_msg + "\n"
						fmt.Fprintf(outgoing_sender_con, juice_msg)
						PresentMutex.Unlock()

						break
					}
					PresentMutex.Unlock()
				}
			} else {
				index = leaveid
				for i := 0; i < numOfServer; i++ {
					index = (index + 1) % numOfServer
					PresentMutex.Lock()
					if present[index] == 1 && JuiceNodes[index] == 0 {
						JuiceNodes[index] = 1
						JuiceJobs[index] = JuiceJobs[index] + cum_job_msg
						fmt.Println("sending to ", index)
						outgoing_sender_con := OutgoingMessagesConns[index]
						juice_msg := "REJUICE " + sdfs_intermediate_filename_prefix + " " + juice_exe + " " + sdfs_dest_filename + " " + strconv.Itoa(deleteJuiceFiles) + " " + cum_job_msg + "\n"
						fmt.Fprintf(outgoing_sender_con, juice_msg)
						PresentMutex.Unlock()
						break
					}
					PresentMutex.Unlock()
				}
			}
			//end
			//reset all juicenode and juicejobs that are not present!
			for i := 0; i < numOfServer; i++ {
				PresentMutex.Lock()
				if present[i] == 0 {
					JuiceNodes[i] = 0
					JuiceJobs[i] = ""
				}
				PresentMutex.Unlock()
			}
			JuiceJobs[leaveid] = ""
		}
		JuiceNodesMutex.Unlock()
	}

	for i := 0; i < 10; i++ {
		if failedNode[i] == true {
			fileTable[i] = nil
		}
	}

	if jsonId == Master {
		println("handle maple failure")
		cur_node := jsonId
		mapleRecorderMutex.Lock()
		for i := 0; i < 10; i++ {
			//if failedNode[i] == true {
			if failedNode[i] == true && mapleRecorder[i] != 0 {
				fmt.Println("Redo Maple Task", i)
				fmt.Println("take over job", cur_node)
				mapleRecorder[i] = 0
				mapleRecorder[cur_node] += 1
				OutgoingMessagesConns_Mutex.Lock()
				outgoing_sender_con := OutgoingMessagesConns[cur_node]
				OutgoingMessagesConns_Mutex.Unlock()

				fmt.Fprintf(outgoing_sender_con, mapleTask[i])
				time.Sleep(500 * time.Millisecond)

				fileSize := len(mapleTaskFiles[i])
				fmt.Println("fileSize", fileSize)
				fileSizeStr := strconv.Itoa(fileSize)

				fmt.Fprintf(outgoing_sender_con, fileSizeStr+"\n")
				//Need ACK?
				for z := 0; z < len(mapleTaskFiles[i]); z++ {
					fmt.Fprintf(outgoing_sender_con, mapleTaskFiles[i][z]+"\n")
				}
				cur_node = findsuccessor(cur_node)
			}
		}
		mapleRecorderMutex.Unlock()
	}

	MasterMutex.Lock()
	fmt.Println("Back to normal")
	underFailure = false
	for i := 0; i < 10; i++ {
		failedNode[i] = false
	}
	MasterMutex.Unlock()
}
