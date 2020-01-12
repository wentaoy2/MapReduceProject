package main

import (
	"fmt"
	"os"
	"strconv"
)

func main() {
	failedNode = make(map[int]bool)
	//process the arguments and pick introducer
	introducer := main_init()
	//read configuration from JSON
	readConfig()
	//setup client threads
	init_maple()
	clientsetup(introducer)
	fmt.Println("Finish Setup Client")
	//close channel
	for i := 0; i < sizeOfNeighbor; i += 1 {
		defer close(NeighborChannel[i])
	}
	// open log file using READ & WRITE permission
	var err error
	path := "vm" + strconv.Itoa(jsonId) + ".log"
	fmt.Println(path)
	createFile(path)
	file, err = os.OpenFile(path, os.O_RDWR, 0644)
	if isError(err) {
		return
	}
	defer file.Close()
	//set up server thread
	//the main thread would start process stdin
	serversetup()

}
