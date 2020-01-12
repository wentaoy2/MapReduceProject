package main

import (
	"fmt"
	"hash/fnv"
	"math/rand"
	"net"
)

/*
Here we have auxillary functions for our master code.
They initialize the master, clear buffers, finds Minimum between numbers
and handles hashing the file to the appropriate active VMs
*/

//We initialize all of the variables used in master here
func init_master() {
	last_processed = make(map[string]int)
	being_processed = make(map[string]int)
	files_present = make(map[string]int)
	latest_node_with_file = make(map[string]int)
	IncomingMessagesConns = make([]net.Conn, numOfServer)
	OutgoingMessagesConns = make([]net.Conn, numOfServer)
	outgoing_updates = make(chan job)
	max_replicas = 4
	majority = 3
	minute = 60
	localaddress = "127.0.0.1"
}

//This is used for read buffers for TCP messages.
func clearbuf(buf []byte) {
	for i := 0; i < len(buf); i++ {
		buf[i] = 0
	}
}

// Min returns the smaller of x or y.
func Min(x, y int) int {
	if x > y {
		return y
	}
	return x
}

//We find a set of 3 or less VMs (Depending on the number of active VMs)
//That act as the quorum in which we send UPDATE PUTS for a file
func randomizeVMnums(curr_vm_conns []int, rand_vm_conns []int) {
	aliveMutex.Lock()
	num_nodes_alive := aliveNode
	aliveMutex.Unlock()
	var rand_nums [4]int
	fmt.Println("num_nodes_alive", num_nodes_alive)
	iter := Min(max_replicas-1, num_nodes_alive)
	for i := 0; i < max_replicas-1; i++ {
		rand_vm_conns[i] = -1
	}
	for i := 0; i < 4; i++ {
		rand_nums[i] = -1
	}

	min := 0
	max := 4

	i := 0
	for true {
		if i >= iter {
			break
		} else {
			//get new rand number
			rand_num := (rand.Intn(max-min) + min)
			if (rand_num != rand_nums[0]) && (rand_num != rand_nums[1]) && (rand_num != rand_nums[2]) && (rand_num != rand_nums[3]) && curr_vm_conns[rand_num] != -1 {
				rand_vm_conns[i] = curr_vm_conns[rand_num]
				rand_nums[i] = rand_num
				i = i + 1
			}
		}
	}

	return
}

//pass in name and it will hash to the VM id that is closest and greater than the hash
func hashfile(filename string) int {
	//make a hash
	h := fnv.New32a()
	h.Write([]byte(filename))
	hashed_val := h.Sum32() % 10
	for i := 0; i < numOfServer; i++ {
		PresentMutex.Lock()
		if present[hashed_val] == 1 {
			PresentMutex.Unlock()
			break
		} else {
			hashed_val = (hashed_val + 1) % 10
		}
		PresentMutex.Unlock()
	}
	return int(hashed_val)
}

//Wrapper that obtains all VM Ids that will contain the file for PUT requests (first put)
func getFileLocation(filename string, curr_vm_conns []int) {
	aliveMutex.Lock()
	num_nodes_alive := aliveNode
	aliveMutex.Unlock()
	//fmt.Println("getting file location/num_nodes_alive ", num_nodes_alive)
	hashed_vm_num := hashfile(filename)
	//fmt.Println("hashed vm_num ", hashed_vm_num)

	for i := 0; i < max_replicas; i++ {
		curr_vm_conns[i] = -1
	}
	if num_nodes_alive > 4 {

		curr_vm_conns[0] = hashed_vm_num
		curr_vm_conns[1] = findsuccessor(hashed_vm_num)
		curr_vm_conns[2] = findsuccessor(curr_vm_conns[1])
		curr_vm_conns[3] = findsuccessor(curr_vm_conns[2])

	} else {
		//fmt.Println("find file locations for less than or equal to 4 nodes")

		for i := 0; i < num_nodes_alive; i++ {

			if i == 0 {
				curr_vm_conns[i] = hashed_vm_num

			} else if i == 1 {
				curr_vm_conns[i] = findsuccessor(hashed_vm_num)

			} else if i == 2 {
				curr_vm_conns[i] = findsuccessor(curr_vm_conns[1])
			}
		}
	}
	//fmt.Println("finished getting file locations")
	return
}
