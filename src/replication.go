package main

import (
	"fmt"
)

/*For each failed node we find a file that it has and
//contact one node that has it to send it to a successor
of the failed node that doesn't have the file*/

func restoreData(outgoing_updates chan job) {
	aliveMutex.Lock()
	var failed_idx int
	if aliveNode != 1 {
		aliveMutex.Unlock()
		//go through all failed nodes
		for i := 0; i < 10; i++ {
			//go through all files for that node
			//FileMutex.Lock() //TODO: IS THIS A SAFE USE OF THE MUTEX
			//if this node failed replicate all files in node once
			if failedNode[i] == true {
				failed_idx = i
				if fileTable[failed_idx] == nil {
					fileTable[failed_idx] = map[string]int{}
				}
				for filename, _ := range fileTable[failed_idx] {
					fmt.Println("replicated file:", filename)
					//find node with file
					var vm_has_file int
					for j := 0; j < numOfServer; j++ {
						if fileTable[j] != nil && fileTable[j][filename] == 1 && failedNode[j] == false {
							vm_has_file = j
							fmt.Println("vm has file: ", j)
							break
						}
					}
					//find first successor of failed thread (check_vm) without file that is present and send file
					check_vm := failed_idx + 1
					for i := 0; i < 10; i++ {
						PresentMutex.Lock()

						if present[check_vm] == 1 {
							if fileTable[check_vm] == nil {
								fileTable[check_vm] = map[string]int{}
							}
						}
						_, isThere := fileTable[check_vm][filename]
						if !isThere && present[check_vm] == 1 {
							PresentMutex.Unlock()

							//vm_has_file sends to check_vm
							fmt.Println("vm with file: ", vm_has_file, " sending to ", check_vm)
							//todo maybe changing order of these will fix problem?
							sendPUTupdates(outgoing_updates, filename, check_vm) //broadcast to everyone that check_vm has the replicated file now

							sendPUTfile(outgoing_updates, filename, check_vm, vm_has_file)

							fileTable[check_vm][filename] = 1
							//update latest_node_with_file
							files_present_Mutex.Lock()
							files_present[filename] = 1
							files_present_Mutex.Unlock()
							latest_node_with_file_Mutex.Lock()
							latest_node_with_file[filename] = check_vm
							latest_node_with_file_Mutex.Unlock()
							fmt.Println("SENT putfile and update for one file, breaking")

							break
						} else {
							check_vm = (check_vm + 1) % numOfServer
							PresentMutex.Unlock()

						}

					}
					fmt.Println("getting another file")

				}
				fmt.Println("done going through all files of node: ", failed_idx)

			}
			//FileMutex.Unlock()
			//update latest_node_with_file
		}
		fmt.Println("done going through all failed nodes")
		return
	} else {
		aliveMutex.Unlock()
		return
	}
}
