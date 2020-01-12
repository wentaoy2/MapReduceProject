package main

import (
	"bytes"
	"fmt"
	"math/rand"
	"net"
	"strconv"
	"time"
)

/*
Check error
*/
func check(e error) {
	if e != nil {
		panic(e)
	}
}

/*
When a node fails because of a timeout we call
this function which sends a LEAVE message to neigbhboring nodes
*/
func nodeFail(leaveid int) {


	broadcastThisMessage := removenode(leaveid)
	elapsed := time.Since(start)

	newMessage := " " + " " + " " + " LEAVE " + fmt.Sprintf("%d", elapsed.Milliseconds())
	byteOutput := []byte(newMessage)
	byteOutput[0] = byte(jsonId)
	byteOutput[2] = byte(leaveid)

	if broadcastThisMessage {
		go handleFailure(leaveid)
		for i := 0; i < len(Neighbor); i += 1 {
			NeighborMutex.Lock()
			if Neighbor[i] != -1 && Neighbor[i] != leaveid {
				MessageMutex[i].Lock()
				NeighborMessageTable[i][elapsed.Milliseconds()] = string(byteOutput)
				MessageMutex[i].Unlock()
				NeighborMutex.Unlock()

			} else {
				NeighborMutex.Unlock()
			}
		}
	}

}

/*
We send heartbeats to our neighbor every second
*/
func heartbeatGenerator(index int) {
	newMessage := " " + " " + " " + " HEARTBEAT"
	byteOutput := []byte(newMessage)
	byteOutput[0] = byte(jsonId)
	tempString := string(byteOutput)
	for {
		time.Sleep(1000 * time.Millisecond)
		NeighborChannel[index] <- tempString

	}
}

/*
This function handles timeouts for each neighbors
*/
func heartbeat(t time.Duration, senderNumber int) func(cancel int) {
	hb := make(chan int)
	go func() {
		for {
			select {
			case cancel := <-hb:
				if cancel == -1 {
					return
				}
			case <-time.After(t):
				HeartbeatSenderMutex.Lock()
				if HeartbeatSender[senderNumber] == -1 {
					HeartbeatSenderMutex.Unlock()
					continue
				} else {
					tempNumber := HeartbeatSender[senderNumber]
					HeartbeatSenderMutex.Unlock()
					writeFile("Node timeout: " + string(senderNumber))

					tempConn, err := net.Dial("tcp", fmt.Sprintf("%s:%s", Addr[tempNumber], Port[tempNumber]))

					if err == nil {
						tempConn.Close()
					} else {
						if tempNumber != jsonId {
							nodeFail(tempNumber)
						}
					}

				}
			}
		}
	}()
	return func(cancel int) {
		select {
		case hb <- cancel:
		default:
		}
	}
}

/*
We send ACK message to neighbors that sent an ADD or LEAVE
*/
func acknowledge(back int, timestamp int64, con *net.UDPConn, addr *net.UDPAddr, port string) {
	tempString := " " + " " + " " + " ACK " + fmt.Sprintf("%d", timestamp)
	byteOutput := []byte(tempString)
	byteOutput[0] = byte(jsonId)
	addr.Port, _ = strconv.Atoi(port)
	con.WriteTo(byteOutput, addr)
}

/*
Here we resend messages for ADD or LEAVE if they are dropped.
We also remove messages that have been ACK'ed
*/
func broadCast() {
	for {
		for i := 0; i < sizeOfNeighbor; i += 1 {
			MessageMutex[i].Lock()
			if len(NeighborMessageTable[i]) != 0 {
				for _, v := range NeighborMessageTable[i] {
					NeighborChannel[i] <- v
				}
			}
			MessageMutex[i].Unlock()
		}
		time.Sleep(1 * time.Second)
	}
}

/*
This is the main server code
that receives and handles messages for ACK, LEAVE, JOIN,
and HEARTBEAT.
*/
func handleUDPconnection() {
	//create a file to write to
	//but doesnt write to an already existing file (idt it does)
	PORT := ":" + Port[jsonId]
	s, err := net.ResolveUDPAddr("udp4", PORT)
	if err != nil {
		fmt.Println(err)
		return
	}
	//create server connection
	udpConn, err := net.ListenUDP("udp4", s)
	fmt.Println("Server Listening:", Port[jsonId])

	if err != nil {
		fmt.Println(err)
		return
	}
	defer udpConn.Close()

	buffer := make([]byte, 1024)
	for {
		elapsed := time.Since(start)

		LeaveMutex.Lock()
		if leave {
			LeaveMutex.Unlock()
			elapsed = time.Since(start)
			newMessage := " " + " " + " " + " LEAVE " + fmt.Sprintf("%d", elapsed.Milliseconds())
			byteOutput := []byte(newMessage)
			byteOutput[0] = byte(jsonId)
			byteOutput[2] = byte(jsonId)
			//just send once, otherwise neighbors timeout
			for i := 0; i < sizeOfNeighbor; i += 1 {
				NeighborChannel[i] <- string(byteOutput)
			}
			return
		}
		LeaveMutex.Unlock()

		for i := 0; i < len(buffer); i++ {
			buffer[i] = 0
		}

		bytesRead, addr, _ := udpConn.ReadFromUDP(buffer)
		indexOfZero := bytes.IndexByte(buffer[4:], 0) + 4

		elapsed = time.Since(start)

		senderid := int(buffer[0])
		messageid := int(buffer[2])

		//interpret message from neighbor
		if string(buffer[4:7]) == "ACK" {
			join_msg := "ACK " + fmt.Sprintf("%d", elapsed.Milliseconds()) + " " + strconv.Itoa(bytesRead) + "\n" //strconv.Itoa(elapsed.Seconds())
			writeFile(join_msg)
			tempId, err := strconv.ParseInt(string(buffer[8:indexOfZero]), 0, 64)
			check(err)
			NeighborMutex.Lock()
			for i := 0; i < sizeOfNeighbor; i += 1 {
				if senderid == Neighbor[i] {
					MessageMutex[i].Lock()
					if _, ok := NeighborMessageTable[i][tempId]; ok {
						delete(NeighborMessageTable[i], tempId)
					}
					MessageMutex[i].Unlock()
				}
			}
			NeighborMutex.Unlock()

		} else if string(buffer[4:13]) == "HEARTBEAT" {
			//false positive tests
			percent_failure := 0.0
			rand.Seed(time.Now().UnixNano())

			rand := rand.Float64()
			if rand > percent_failure {
				join_msg := "HEARTBEAT " + fmt.Sprintf("%d", elapsed.Milliseconds()) + " " + strconv.Itoa(bytesRead) + "\n"
				writeFile(join_msg)
				PresentMutex.Lock()
				if present[senderid] == 0 {
					PresentMutex.Unlock()
					addnode(senderid)
					newMessage := " " + " " + " " + " ADD " + fmt.Sprintf("%d", elapsed.Milliseconds())
					byteOutput := []byte(newMessage)
					byteOutput[0] = byte(jsonId)
					byteOutput[2] = byte(senderid)
					NeighborMutex.Lock()
					for i := 0; i < sizeOfNeighbor; i += 1 {
						if Neighbor[i] != -1 && Neighbor[i] != senderid {
							MessageMutex[i].Lock()
							NeighborMessageTable[i][elapsed.Milliseconds()] = string(byteOutput)
							MessageMutex[i].Unlock()
						}
					}
					NeighborMutex.Unlock()
				} else {
					PresentMutex.Unlock()
				}

				HeartbeatSenderMutex.Lock()
				for i := 0; i < sizeOfNeighbor; i += 1 {
					if senderid == HeartbeatSender[i] {
						heartbeatCountdown[i](0)
						break
					}
				}
				HeartbeatSenderMutex.Unlock()
			} else {
				fmt.Println("RANDOM DROP MESSAGE")
			}
		} else if string(buffer[4:8]) == "JOIN" {
			join_msg := "JOIN " + fmt.Sprintf("%d", elapsed.Milliseconds()) + " " + strconv.Itoa(bytesRead) + "\n"
			writeFile(join_msg)

			tempString := ""

			//Add Master [Only used in MP3]
			tempString += strconv.Itoa(Master)
			tempString += " "

			PresentMutex.Lock()
			for i := 0; i < numOfServer; i++ {
				if present[i] != 0 {
					tempString += strconv.Itoa(i)
					tempString += " "
				}
			}

			tempString = tempString[:len(tempString)-1]
			PresentMutex.Unlock()

			join_msg = "MEM " + fmt.Sprintf("%d", elapsed.Milliseconds()) + " " + strconv.Itoa(len(tempString)) + "\n"
			writeFile(join_msg)

			byteOutput := []byte(tempString)
			_, err := udpConn.WriteTo(byteOutput, addr)
			check(err)
		} else if string(buffer[4:9]) == "LEAVE" {

			join_msg := "LEAVE " + fmt.Sprintf("%d", elapsed.Milliseconds()) + " " + strconv.Itoa(bytesRead) + "\n"
			writeFile(join_msg)
			//check if a node leaves and tell  client on same machine
			leaveid := messageid

			broadcastThisMessage := removenode(leaveid)
			identifier, err := strconv.ParseInt(string(buffer[10:indexOfZero]), 0, 64)
			check(err)
			if broadcastThisMessage {
				go handleFailure(leaveid)
				buffer[0] = byte(jsonId)
				NeighborMutex.Lock()
				for i := 0; i < sizeOfNeighbor; i += 1 {

					if (Neighbor[i] != senderid) && (Neighbor[i] != -1) && Neighbor[i] != leaveid {
						MessageMutex[i].Lock()
						NeighborMessageTable[i][identifier] = string(buffer[:indexOfZero])
						MessageMutex[i].Unlock()
					}
				}
				NeighborMutex.Unlock()
			}
			acknowledge(senderid, identifier, udpConn, addr, Port[senderid])
		} else if string(buffer[4:7]) == "ADD" {
			addid := messageid
			join_msg := "ADD " + fmt.Sprintf("%d", elapsed.Milliseconds()) + " " + strconv.Itoa(bytesRead) + "\n"
			writeFile(join_msg)

			broadcastThisMessage := addnode(addid)
			identifier, err := strconv.ParseInt(string(buffer[8:indexOfZero]), 0, 64)
			check(err)
			if broadcastThisMessage {
				buffer[0] = byte(jsonId)
				NeighborMutex.Lock()
				for i := 0; i < sizeOfNeighbor; i += 1 {
					if (Neighbor[i] != senderid) && (Neighbor[i] != -1) && (Neighbor[i] != addid) {
						MessageMutex[i].Lock()
						NeighborMessageTable[i][identifier] = string(buffer[:indexOfZero])
						MessageMutex[i].Unlock()
					}
				}
				NeighborMutex.Unlock()
			}
			acknowledge(senderid, identifier, udpConn, addr, Port[senderid])
		}
	}
}

/*
create connections to neighbors to
send messages to
*/
func UDPNeighborSender(index int) {
	for {
		currentJob := <-NeighborChannel[index]
		if currentJob == "STOP" {
			break
		}
		NeighborMutex.Lock()
		if Neighbor[index] != -1 {

			fmt.Fprintf(NeighborConnection[index], currentJob)
			NeighborMutex.Unlock()
			continue
		}
		NeighborMutex.Unlock()

	}
}
