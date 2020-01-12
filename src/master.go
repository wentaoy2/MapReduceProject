/*
master code:  The master processes all operations first before
a node PUTs/GETs/DELETEs a file.
*/
package main

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

//using present array and present mutexes
//addr array, and numofservers
//and current addr, alive node

/*
The following two functions queues outgoing PUT/DELETE update messages for all active nodes.
*/
//We let all nodes know that a file has been deleted
func sendDELETEupdates(outgoing_updates chan job, filename string, node_num int) {
	for i := 0; i < numOfServer; i++ {
		PresentMutex.Lock()
		if present[i] == 1 {
			temp := job{operation: "UPDATE DELETE", filename: filename, replica_node: node_num, serverId: i}
			outgoing_updates <- temp
		}
		PresentMutex.Unlock()
	}
}

func sendDELETEfile(outgoing_updates chan job, filename string, node_num int, server_id int) {
	PresentMutex.Lock()
	if present[server_id] == 1 {
		temp := job{operation: "DELETE", filename: filename, replica_node: node_num, serverId: server_id}
		outgoing_updates <- temp
	}
	PresentMutex.Unlock()

}

//Here we let all nodes know that a file has been added for the first time to node_num
func sendPUTupdates(outgoing_updates chan job, filename string, node_num int) {
	//fmt.Println("sending PUT update")
	for i := 0; i < numOfServer; i++ {
		PresentMutex.Lock()
		if present[i] == 1 {
			temp := job{operation: "UPDATE PUT", filename: filename, replica_node: node_num, serverId: i}
			outgoing_updates <- temp
		}
		PresentMutex.Unlock()
	}
}

func sendPUTfile(outgoing_updates chan job, filename string, node_num int, server_id int) {
	temp := job{operation: "PUT", filename: filename, replica_node: node_num, serverId: server_id}
	outgoing_updates <- temp
}

/*we deal with GET and DELETE here.
For a GET we send back the latest node that has the file
For a DELETE we send a DELETE update to all active nodes and remove the file from
our hashmaps
*/
func handleGETandDELETE(operation_requestor_con net.Conn, operation string, rand_vm_conns []int, filename string, outgoing_updates chan job) {
	//var err error
	for {
		being_processed_Mutex.Lock()
		being_processed_val := being_processed[filename]
		being_processed_Mutex.Unlock()
		if operation == "GET" && being_processed_val != 1 {
			fmt.Fprintf(operation_requestor_con, strconv.Itoa(latest_node_with_file[filename])+"\n")
			fmt.Println("done with GET request")
			break
		} else if operation == "DELETE" { //"delete [filename]"
			//send DELETE to nodes that have the file
			fmt.Println("SENDING DELETE MESSAGES")
			for i := 0; i < numOfServer; i++ {
				//check if node has file
				FileMutex.Lock()
				if fileTable[i][filename] == 1 {
					FileMutex.Unlock()
					sendDELETEfile(outgoing_updates, filename, i, i)
					sendDELETEupdates(outgoing_updates, filename, i)
					FileMutex.Lock()
					delete(fileTable[i], filename)
					FileMutex.Unlock()

				} else {
					FileMutex.Unlock()
				}
			}
			//remove all traces of file
			files_present_Mutex.Lock()
			delete(files_present, filename)
			files_present_Mutex.Unlock()

			delete(latest_node_with_file, filename)
			fmt.Println("DONE REMOVING FILE AND SENDING MESSAGES")

			break
		} else {
			fmt.Println("File PUT not finished")
		}
	}
}

/*
If a node requests a PUT for a file that another node requested a PUT for
within a minute, then we alert the requestor.  He can either choose to send the file, reject,
or timeout if he doesn't respond
*/

func handleconfirmation(operation_requestor_con net.Conn, filename string, rand_vm_conns []int) {
	timeoutDurationSize := 30 * time.Second
	operation_requestor_con.SetReadDeadline(time.Now().Add(timeoutDurationSize))
	reader := bufio.NewReader(operation_requestor_con)
	//send confirmation to requesting machine
	fmt.Fprintf(operation_requestor_con, "CONFIRM"+"\n")
	read_buffer, err := reader.ReadString('\n')
	if err != nil {
		//if timeout we just quit the update
		fmt.Println("Time out on PUT for ", filename)
		operation_requestor_con.SetReadDeadline((time.Time{}))
		return
	}
	read_buffer = read_buffer[:len(read_buffer)-1]
	operation_requestor_con.SetReadDeadline((time.Time{}))

	request := strings.Fields(read_buffer)
	fmt.Println("request: ", request[0])

	if request[0] == "Y" { //"accept confirmation"
		fmt.Println("Received Y")
		fmt.Println("Subsequent PUT for ", filename)
		being_processed_Mutex.Lock()
		being_processed[filename] = 1
		being_processed_Mutex.Unlock()
		num_nodes := 0
		for i := 0; i < max_replicas-1; i++ {
			if rand_vm_conns[i] == -1 {
				break
			} else {
				num_nodes = num_nodes + 1
			}
		}
		fmt.Fprintf(operation_requestor_con, strconv.Itoa(num_nodes)+"\n")
		for i := 0; i < max_replicas-1; i++ {
			if rand_vm_conns[i] == -1 {
				break
			} else {
				fmt.Println("sending to ", rand_vm_conns[i])
				fmt.Fprintf(operation_requestor_con, strconv.Itoa(rand_vm_conns[i])+"\n")
			}
		}
		last_processed_Mutex.Lock()
		last_processed[filename] = int(time.Now().Unix())
		last_processed_Mutex.Unlock()
		latest_node_with_file[filename] = rand_vm_conns[0]
		being_processed_Mutex.Lock()
		being_processed[filename] = 0
		being_processed_Mutex.Unlock()

	} else if request[0] == "N" { //"REJECT confirmation"
		fmt.Println("refused confirmation")
	} else {
		fmt.Println("Something else went wrong")
	}
	return
	//return
}

/*
This is the handler for outgoing messages.  Update messages are queued on first PUT/DELETE
requests. They are dequeued here and sent to nodes that are alive
*/
func handlefilenameupdate(outgoing_updates chan job) {
	for true {
		currentJob := <-outgoing_updates
		OutgoingMessagesConns_Mutex.Lock()
		outgoing_sender_con := OutgoingMessagesConns[currentJob.serverId]
		OutgoingMessagesConns_Mutex.Unlock()
		//fmt.Printf("Talking to %s\n", outgoing_sender_con.RemoteAddr().String())
		if currentJob.operation == "UPDATE PUT" {
			//	fmt.Println("sending PUT UPDATE")
			//	fmt.Println("sending request: ", "UPDATE PUT "+strconv.Itoa(currentJob.replica_node)+" "+(currentJob.filename)+"\n")
			fmt.Fprintf(outgoing_sender_con, "UPDATE PUT "+strconv.Itoa(currentJob.replica_node)+" "+(currentJob.filename)+"\n")
		} else if currentJob.operation == "PUT" {
			fmt.Fprintf(outgoing_sender_con, "PUT "+strconv.Itoa(currentJob.replica_node)+" "+(currentJob.filename)+"\n")
		} else if currentJob.operation == "UPDATE DELETE" { //delete
			//	fmt.Println("sending DELETE UPDATE")
			fmt.Fprintf(outgoing_sender_con, "UPDATE DELETE "+strconv.Itoa(currentJob.replica_node)+" "+(currentJob.filename)+"\n")
		} else {
			//	fmt.Println("sending DELETE file")
			fmt.Fprintf(outgoing_sender_con, "DELETE "+(currentJob.filename)+"\n")

		}
	}
}

/*
This function determines whether the PUT is within a minute of another PUT request to the same file,
the first PUT request for a file, or a request after a minute of another PUT request (a subsequent PUT request)
*/
func handleput(operation_requestor_con net.Conn, filename string, rand_vm_conns []int, curr_vm_conns []int, outgoing_updates chan job) {
	//var err error
	aliveMutex.Lock()
	majority = Min(majority, aliveNode)
	aliveMutex.Unlock()
	//mutextes for what variables
	//if not first put and within 1 minute of another put query
	last_processed_Mutex.Lock()
	last_processed_val := last_processed[filename]
	last_processed_Mutex.Unlock()
	files_present_Mutex.Lock()
	files_present_val := files_present[filename]
	files_present_Mutex.Unlock()
	if (files_present_val == 1) && (int(time.Now().Unix())-last_processed_val < minute) {
		fmt.Println("PUT within a minute to file")
		handleconfirmation(operation_requestor_con, filename, rand_vm_conns)
		return
	} else if (files_present_val == 1) && (int(time.Now().Unix())-last_processed_val > minute) { //file present and last query to same file happened more than a minute ago
		//send all nodes to transfer to
		fmt.Println("Subsequent PUT for ", filename)
		being_processed_Mutex.Lock()
		being_processed[filename] = 1
		being_processed_Mutex.Unlock()
		num_nodes := 0
		for i := 0; i < max_replicas-1; i++ {
			if rand_vm_conns[i] == -1 {
				break
			} else {
				num_nodes = num_nodes + 1
			}
		}
		fmt.Fprintf(operation_requestor_con, strconv.Itoa(num_nodes)+"\n")
		for i := 0; i < max_replicas-1; i++ {
			if rand_vm_conns[i] == -1 {
				break
			} else {
				fmt.Fprintf(operation_requestor_con, strconv.Itoa(rand_vm_conns[i])+"\n")
			}
		}
		last_processed_Mutex.Lock()
		last_processed[filename] = int(time.Now().Unix())
		last_processed_Mutex.Unlock()
		latest_node_with_file[filename] = rand_vm_conns[0]
		being_processed_Mutex.Lock()
		being_processed[filename] = 0
		being_processed_Mutex.Unlock()
		return
	} else if files_present_val != 1 { //update last_processed with query time and it's the first put
		//mark for back to back requests to same file
		fmt.Println("first PUT for ", filename)
		last_processed_Mutex.Lock()
		last_processed[filename] = int(time.Now().Unix())
		last_processed_Mutex.Unlock()
		files_present_Mutex.Lock()
		files_present[filename] = 1
		files_present_Mutex.Unlock()
		being_processed_Mutex.Lock()
		being_processed[filename] = 1
		being_processed_Mutex.Unlock()
		num_nodes := 0
		for i := 0; i < max_replicas; i++ {
			if curr_vm_conns[i] == -1 {
				break
			} else {
				num_nodes = num_nodes + 1
			}
		}
		fmt.Fprintf(operation_requestor_con, strconv.Itoa(num_nodes)+"\n")
		for i := 0; i < max_replicas; i++ {
			if curr_vm_conns[i] == -1 {
				break
			} else {
				fmt.Println("sending to ", curr_vm_conns[i])
				fmt.Fprintf(operation_requestor_con, strconv.Itoa(curr_vm_conns[i])+"\n")
				sendPUTupdates(outgoing_updates, filename, curr_vm_conns[i])
			}
		}
		being_processed_Mutex.Lock()
		being_processed[filename] = 0
		being_processed_Mutex.Unlock()
		latest_node_with_file[filename] = curr_vm_conns[0]
		return
	} else {
		fmt.Println("something bad went wrong in handling PUTs")
		return
	}
}

func handlemessages(operation_requestor_con net.Conn, operation string, filename string, outgoing_updates chan job) {
	//if there are more than 4 nodes alive contact 3
	curr_vm_conns := make([]int, max_replicas)
	rand_vm_conns := make([]int, max_replicas-1)
	getFileLocation(filename, curr_vm_conns)
	randomizeVMnums(curr_vm_conns, rand_vm_conns)
	for i := 0; i < max_replicas-1; i++ {
		fmt.Println("chose randome vm: ", rand_vm_conns[i], "for ", operation)
	}
	if operation == "PUT" {
		fmt.Println("handling put")
		handleput(operation_requestor_con, filename, rand_vm_conns, curr_vm_conns, outgoing_updates)
	} else { //handle GET and DELETE messages
		handleGETandDELETE(operation_requestor_con, operation, rand_vm_conns, filename, outgoing_updates)
	}
	return
}

//listen fo incoming GET/PUT/DELETE messages
//and call the message handler
func handleIncomingConnections(conn net.Conn, outgoing_updates chan job) {
	fmt.Printf("Talking to %s\n", conn.RemoteAddr().String())
	reader := bufio.NewReader(conn)
	defer conn.Close()
	for true {

		var err error
		read_buffer, err := reader.ReadString('\n')
		if err != nil {
			fmt.Println("can't read inside handleIncomingConnections ", err)
			break
		}
		read_buffer = read_buffer[:len(read_buffer)-1]

		request := strings.Fields(read_buffer)

		fmt.Println(string(read_buffer))

		var filename string
		//handle the message
		//"GET [filename]"
		if request[0]+" "+request[1] == "MAS GET" {
			fmt.Println("GOT GET")
			filename = string(read_buffer[8:])
			//ask up to 3 replicas for copy
			handlemessages(conn, "GET", filename, outgoing_updates)
			//send back vm node number with latest file
		} else if request[0]+" "+request[1] == "MAS PUT" { //"PUT [filename]"
			fmt.Println("GOT PUT")

			filename = string(read_buffer[8:])
			handlemessages(conn, "PUT", filename, outgoing_updates)
		} else if request[0]+" "+request[1] == "MAS DELETE" { //"delete [filename]"
			fmt.Println("GOT DELETE")

			filename = string(read_buffer[11:])
			handlemessages(conn, "DELETE", filename, outgoing_updates)
		} else if request[0]+" "+request[1] == "MAS JUICE" { //START

			juice_start_time = int32(time.Now().Unix())
			num_juice_jobs, _ := strconv.Atoi(request[3])
			NumJuiceJobs = num_juice_jobs
			fmt.Println("received JUICE command for file ", request[4], " for ", num_juice_jobs, " jobs running ", request[2])
			//check if there are enough nodes alive
			aliveMutex.Lock()
			num_alive_nodes := aliveNode
			aliveMutex.Unlock()
			if num_alive_nodes < num_juice_jobs {
				fmt.Println("specified too many juice jobs than there are nodes available.", num_juice_jobs, "number of nodes alive: ", num_alive_nodes)
				return
			}

			//clear our juicejobs and juicenodes
			for i := 0; i < numOfServer; i++ {
				JuiceNodes[i] = 0
				JuiceJobs[i] = ""
			}

			//received MAST JUICE <juice_exe> <num_juices>
			//<sdfs_intermediate_filename_prefix> <sdfs_dest_filename> delete_input={0,1}
			juice_exe = "./" + request[2]
			sdfs_intermediate_filename_prefix = request[4]
			sdfs_dest_filename = request[5]
			//open and clear a local dest_file for intermediate juice results

			//delete_input={0,1}
			findJuiceNodes(num_juice_jobs)
			//determine the <num_juice> nodes that will get jobs
			//update JuiceNodes
			//partition the data to these nodes
			//update JuiceJobs
			getJuiceJobs(sdfs_intermediate_filename_prefix, num_juice_jobs)
			//send them messages of the format
			//JUICE [filename prefix] [reduce_exe] [sdfs_dest_filename] [list of keys]
			fmt.Println("numOfServer: ", numOfServer)

			for i := 0; i < numOfServer; i++ {
				if JuiceJobs[i] != "" {
					fmt.Println("Juice Job ", JuiceJobs[i], "for ", i)
				}
			}

			for i := 0; i < numOfServer; i++ {
				if JuiceNodes[i] == 1 {
					outgoing_sender_con := OutgoingMessagesConns[i]
					juice_msg := "JUICE " + sdfs_intermediate_filename_prefix + " " + juice_exe + " " + sdfs_dest_filename + " " + request[6] + " " + JuiceJobs[i] + "\n"
					fmt.Fprintf(outgoing_sender_con, juice_msg)
				}
			}
			fmt.Println("done sending juices")
			//END
		} else if request[0]+" "+request[1] == "MAS JUICEFIN" {

			fin_node, _ := strconv.Atoi(request[2])
			fmt.Println("node: ", fin_node, "finished")
			done := 0
			JuiceNodesMutex.Lock()
			JuiceNodes[fin_node] = 0
			JuiceJobs[fin_node] = ""
			JuiceNodesMutex.Unlock()
			for i := 0; i < numOfServer; i++ {
				JuiceNodesMutex.Lock()
				if JuiceNodes[i] == 1 {
					fmt.Println("node: ", i, "still not done")
					done = 1
				}
				JuiceNodesMutex.Unlock()
			}

			if done == 0 {
				//todo
				//combine all reducer_intermediate_file_prefix_k_files
				combine_files()
				//PUT dest file to sdfs
				PUTJuiceResults(outgoing_updates)
				fmt.Println("juice job finished in ", int32(time.Now().Unix())-juice_start_time)

			}
			//end
		} else if request[0]+" "+request[1] == "MR MAPLE" {

			mapperExeName := request[2]
			numOfMapper, _ := strconv.Atoi(request[3])
			intermediateFileName := request[4]
			inputFileDir := request[5]
			//mapleFileName = intermediateFileName
			numOfFiles := 0
			var filesInDirectory []string
			for k, _ := range latest_node_with_file {
				if strings.HasPrefix(k, inputFileDir) {
					numOfFiles += 1
					filesInDirectory = append(filesInDirectory, k)
				}
			}
			if numOfFiles == 0 {
				continue
			}

			share := numOfFiles / numOfMapper
			if share == 0 {
				share = 1
			}

			counter := 0

			currentNode := jsonId
			fmt.Println("number of processed files", numOfFiles)
			for {

				mapleRecorder[currentNode] = 1

				fin := counter + share
				if fin > numOfFiles {
					fin = numOfFiles
				}

				//PresentMutex.Lock()
				OutgoingMessagesConns_Mutex.Lock()
				outgoing_sender_con := OutgoingMessagesConns[currentNode]
				OutgoingMessagesConns_Mutex.Unlock()
				//PresentMutex.Unlock()
				fileSize := strconv.Itoa(fin - counter)
				fmt.Println("sending file size " + fileSize)
				mapleTask[currentNode] = "MR" + " " + "MAPLE" + " " + mapperExeName + " " + intermediateFileName + " " + inputFileDir + "\n"

				fmt.Fprintf(outgoing_sender_con, "MR"+" "+"MAPLE"+" "+mapperExeName+" "+intermediateFileName+" "+inputFileDir+"\n")

				fmt.Println("current node", currentNode)
				fmt.Println("Send Out", mapleTask[currentNode])

				//Need ACK?

				fmt.Fprintf(outgoing_sender_con, fileSize+"\n")
				//Need ACK?
				mapleTaskFiles[currentNode] = make([]string, fin-counter)
				ct := 0
				for z := counter; z < fin; z++ {
					mapleTaskFiles[currentNode][ct] = filesInDirectory[z]
					fmt.Fprintf(outgoing_sender_con, filesInDirectory[z]+"\n")
					//sendPUTfile(outgoing_updates, filename, check_vm, vm_has_file)
					//temp := job{operation: "PUT", filename: filename, replica_node: node_num, serverId: server_id}
					//outgoing_updates <- temp
					ct++
				}

				//reader := bufio.NewReader(outgoing_sender_con)

				//netData, err := reader.ReadString('\n')

				if fin == numOfFiles {
					break
				}

				counter += share
				currentNode = findsuccessor(currentNode)

			}
			fmt.Println("Master finish maple")
		} else if request[0]+" "+request[1] == "MR TRANSFER" {
			fmt.Println("maple transfer!")
			//mapleMasterMutex.Lock()
			currentNum, err := strconv.Atoi(request[2])
			check(err)
			machineId, err := strconv.Atoi(request[3])
			check(err)
			fileMap := map[string]string{}
			for i := 0; i < currentNum; i++ {
				tempName, err := reader.ReadString('\n')
				if err != nil {
					//		fmt.Println("transfer Failed!\n")
					return
				}
				tempName = tempName[:len(tempName)-1]
				fileMap[tempName] = ""
				var sizeBuf [8]byte
				_, err = io.ReadFull(reader, sizeBuf[:])
				if err != nil {
					//		fmt.Println("transfer Failed!\n")
					return
				}
				size := int(binary.BigEndian.Uint64(sizeBuf[:]))
				//fmt.Println("sizzz", size)
				fmt.Fprintf(conn, "ACK\n")

				buf := make([]byte, size)
				n, err := io.ReadFull(reader, buf)
				if err != nil {
					//		fmt.Println("transfer Failed!\n")
					return
				}
				//check(err)
				fileMap[tempName] += string(buf[:n])

			}
			fmt.Println("Writing")
			writeToFileTempMutex.Lock()
			for localFileName, val := range fileMap {
				//	fmt.Println("Write" + localFileName)
				mapleIntermediateFiles[localFileName] = 1
				f, err := os.OpenFile("mapTemp/"+localFileName, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
				check(err)
				f.WriteString(val)
				f.Close()
			}
			writeToFileTempMutex.Unlock()

			fmt.Println("Distributing")
			mapleRecorderMutex.Lock()
			mapleRecorder[machineId] -= 1
			fmt.Println("Done one")
			done := true
			for i := 0; i < len(mapleRecorder); i++ {
				fmt.Println("test", i, mapleRecorder[i])
				if mapleRecorder[i] != 0 {
					done = false
				}
			}
			mapleRecorderMutex.Unlock()
			if done {
				var wg sync.WaitGroup
				wgCounter := 0
				for interF, _ := range mapleIntermediateFiles {
					curr_vm_conns := make([]int, max_replicas)
					getFileLocation(interF, curr_vm_conns)
					for j := 0; j < len(curr_vm_conns); j++ {
						fmt.Println("Fin", interF)
						//fmt.Println("testFin3", jsonId)
						//fmt.Println("testFin3", curr_vm_conns[j])
						if curr_vm_conns[j] != -1 {

							wg.Add(1)
							if curr_vm_conns[j] == jsonId {
								go copyFileContentsWG("mapTemp/"+interF, "storage/"+interF, true, false, &wg) //go copyFileContents("mapTemp/"+interF, "storage/"+interF, true, true)
								//check(err)
							} else {
								go fileSenderWG(curr_vm_conns[j], "mapTemp/"+interF, "storage/"+interF, true, false, &wg)
								//go fileSender(curr_vm_conns[j], "mapTemp/"+interF, "storage/"+interF, true, true)
							}

							sendPUTupdates(outgoing_updates, interF, curr_vm_conns[j])
							wgCounter++
							if wgCounter == 100 {
								wg.Wait()
								wgCounter = 0
							}
						}
					}
					//println("delete" + interF)
					//err := os.Remove("mapTemp/" + interF)
					//check(err)
				}
				wg.Wait()
			}
			//mapleMasterMutex.Unlock()
			fmt.Println("Maple finally done")
		} else {
			fmt.Println("Invalid operation")
		}
	}
}

//When a node joins it will connect to our listener below
//and then we will connect to two listeners on that node for
//incoming and outgoing messages
func handleTCPconnections() {

	port_num, err := strconv.Atoi(Port[0])
	//fmt.Println("hello: " + Port[0])
	if err != nil {
		fmt.Println("error strconv")
	}

	PORT_1 := strconv.Itoa(port_num + 1)
	PORT_2 := strconv.Itoa(port_num + 2)
	fmt.Println(PORT_1)
	server, err := net.Listen("tcp4", ":"+PORT_1)
	check(err)
	defer server.Close()
	fmt.Println("Server started! Waiting for connections...")
	for {
		connection, err := server.Accept()
		if err != nil {
			fmt.Println("Error: ", err)
			os.Exit(1)
		}
		fmt.Println("Client connected")
		addr := connection.RemoteAddr().String()
		addr = addr[:strings.IndexByte(addr, ':')]
		fmt.Println(addr)

		if addr == localaddress {
			fmt.Println("received message from node:  ", jsonId)
			//time.Sleep(1 * time.Second)
			OutgoingMessagesConns_Mutex.Lock()
			OutgoingMessagesConns[jsonId], err = net.Dial("tcp", fmt.Sprintf("%s:%s", localaddress, PORT_2))
			if err != nil {
				check(err)
				fmt.Println("failed create outgoing connections with myself")
			}
			defer OutgoingMessagesConns[jsonId].Close()
			OutgoingMessagesConns_Mutex.Unlock()
			go handleIncomingConnections(connection, outgoing_updates)
			go handlefilenameupdate(outgoing_updates)
		} else {
			for i := 0; i < numOfServer; i++ {
				if addr == Addr[i] {
					fmt.Println("received message from node:  ", i)
					OutgoingMessagesConns_Mutex.Lock()
					OutgoingMessagesConns[i], err = net.Dial("tcp", fmt.Sprintf("%s:%s", Addr[i], PORT_2))
					if err != nil {
						fmt.Println("failed create outgoing connections with node")
					}
					defer OutgoingMessagesConns[i].Close()
					OutgoingMessagesConns_Mutex.Unlock()
					go handleIncomingConnections(connection, outgoing_updates)
					go handlefilenameupdate(outgoing_updates)
					break
				}
			}
		}
	}
}

//main function to be called if a node has been elected as master
func master() {
	init_master()
	//establish all connections to active machines and make sure to terminate if other nodes fais and handle messages
	go handleTCPconnections()
	return
}
