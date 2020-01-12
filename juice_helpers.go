package main

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"hash/fnv"
	"io"
	"io/ioutil"
	"log"
	"math/rand"
	"net"
	"os"
	"strconv"
	"time"

	"os/exec"
	"strings"
)

func IsJuiceJob(filename string) bool {
	words := strings.Fields(rejuicejob)
	for i := 0; i < len(words); i++ {
		if words[i] == filename {
			return true
		}
	}
	return false
}
func performJuiceJob(filename string) {
	//JUICE [filename prefix] [reduce_exe] [sdfs_dest_filename] [DELETE = {0,1}] [list of keys]
	reducer_intermediate_filename := "reducer_intermediate_file_rejuice" + strconv.Itoa(jsonId) + ".txt"

	f, _ := os.OpenFile(reducer_intermediate_filename, os.O_APPEND|os.O_WRONLY, os.ModePerm)
	defer f.Close()

	//assumes [reduce_exe] has been built already
	key := filename[len(sdfs_intermediate_filename_prefix)+1 : len(filename)-4]
	fmt.Println("got key files", filename, " key ", key) //)+j], "of size ",request[i+j+1])
	//process the request and save it to local intermediate file
	//pass in [file_name] to [reduce_exe]
	fmt.Println("app: ", juice_exe, " filename ", filename)

	//get the file from sdfs if you dont have it
	if fileExists(filename) {
		fmt.Println("sdfs stroes file locally.  Using that file for reduce")
	} else {
		fmt.Println("file isn't there for soem reason")
	}
	out, err := exec.Command(juice_exe, filename).Output()
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
	if deleteJuiceFiles == 1 {
		err := os.Remove(filename)
		check(err)
	}
	//PUT reducer_intermediate_filename.txt to sdfs [sdfs_dest_filename]
	//send MAS JUICEFIN
	//todo only send juice fin on completion of all jobs

	juiceFiles := strings.Fields(rejuicejob)
	num_rejuice_completed = num_rejuice_completed + 1
	fmt.Println("num of compelted rejuice jobs", num_rejuice_completed)

	if num_rejuice_completed == len(juiceFiles) {
		fmt.Println("Sending JUICEFIN [jsonid] from performJuiceJob")
		juice_msg := "MAS JUICEFIN " + strconv.Itoa(jsonId) + "\n"
		fmt.Fprintf(MasterConnection, juice_msg)
		rejuicejob = ""
		num_rejuice_completed = 0
		fileSender(Master, "reducer_intermediate_file_rejuice"+strconv.Itoa(jsonId)+".txt", "storage/"+"reducer_intermediate_file_rejuice"+strconv.Itoa(jsonId)+".txt",
			false, false)

	}

}

func juice_job_worker(id int, filename string, prefix string, f *os.File) { //wg *sync.WaitGroup,) {
	//defer wg.Done()

	fmt.Printf("Worker %v: Started\n", id)
	fmt.Println("got key files", filename) //)+j], "of size ",request[i+j+1])
	//process the request and save it to local intermediate file
	//pass in [file_name] to [reduce_exe]
	key := filename[len(prefix)+1 : len(filename)-4]
	onlylocal := 0
	//get the file from sdfs if you dont have it
	if fileExists("storage/" + filename) {
		fmt.Println("sdfs stroes file locally.  Using that file for reduce")
		filename = "storage/" + filename
	} else if fileExists(filename) {
		onlylocal = 1
	} else {
		onlylocal = 1
		JuiceGET(filename)
	}
	fmt.Println("app: ", juice_exe, " filename ", filename)

	out, err := exec.Command(juice_exe, filename).Output()
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
		err := os.Remove(filename)
		check(err)
	}
	fmt.Printf("Worker %v: Finished\n", id)

	return
}

//start
// fileExists checks if a file exists and is not a directory before we
// try using it to prevent further errors.
func fileExists(filename string) bool {
	info, err := os.Stat(filename)
	if os.IsNotExist(err) {
		return false
	}
	return !info.IsDir()
}

//end

func JuiceGET(file_name string) {

	filename := file_name
	fmt.Fprintf(MasterConnection, "MAS"+" "+"GET"+" "+filename+"\n")
	reader := bufio.NewReader(MasterConnection)
	netData, err := reader.ReadString('\n')
	if err != nil {
		fmt.Println("MasterConnection Failed!\n")

	}
	//check(err)
	netData = netData[:len(netData)-1]
	nodeID, err := strconv.Atoi(netData)
	check(err)
	localFileName := file_name
	fmt.Println("getting ", file_name, " from ", nodeID)
	_, err = os.Stat(localFileName)
	if err == nil {
		os.Remove(localFileName)
	}
	PresentMutex.Lock()

	if present[nodeID] != 1 {
		//find a new node to get the file from
		for i := 0; i < numOfServer; i++ {
			if present[i] == 1 && fileTable[i][file_name] == 1 {
				nodeID = i

				break
			}
		}
	}
	PresentMutex.Unlock()

	f, err := os.Create(localFileName)
	check(err)
	f.Close()
	//}
	f, err = os.OpenFile(localFileName, os.O_APPEND|os.O_WRONLY, os.ModePerm)
	check(err)

	//decide if we can just get file from local directory
	if nodeID == jsonId {
		now := time.Now() // current local time
		sec := now.UnixNano()
		fmt.Println("Current date and time for curr machine: ", sec)
		err := copyFileContents("storage/"+filename, localFileName, false, false)
		sec = now.UnixNano()
		fmt.Println("Current date and time for curr machine: ", sec)
		check(err)

	} else {

		temperaryConnection, err := net.Dial("tcp", fmt.Sprintf("%s:%s", Addr[nodeID], Port[nodeID]))
		//	}
		fmt.Fprintf(temperaryConnection, "GET"+" "+filename+"\n")
		reader2 := bufio.NewReader(temperaryConnection)
		var sizeBuf [8]byte
		_, err = io.ReadFull(reader2, sizeBuf[:])
		if err != nil {
			fmt.Println("TempConnection Failed!\n")

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
	}
	fmt.Println("go file: ", file_name)

}
func findJuiceNodes(num_juice_jobs int) {
	//get random number between 0 and 9 and find closest id greater than that random number
	//mark that node and num_juice_jobs -1 successor nodes for jobs
	rand_num := rand.Intn(numOfServer - 1)
	fmt.Println("choose random number: ", rand_num)

	index := rand_num
	for i := 0; i < numOfServer; i++ {
		index = (rand_num + i) % numOfServer
		PresentMutex.Lock()
		if present[index] == 1 {
			num_juice_jobs = num_juice_jobs - 1
			JuiceNodes[index] = 1
			PresentMutex.Unlock()
			break
		}
		PresentMutex.Unlock()
	}

	fmt.Println("first juice job ", index)

	for i := 1; i < numOfServer; i++ {
		fmt.Println("num_juice_jobs: ", num_juice_jobs)
		if num_juice_jobs == 0 {
			break
		}
		index = (index + 1) % numOfServer
		fmt.Println("index: ", index)

		PresentMutex.Lock()
		fmt.Println("present: ", present[index])

		if present[index] == 1 {
			num_juice_jobs = num_juice_jobs - 1
			JuiceNodes[index] = 1
		}
		PresentMutex.Unlock()
	}
}

func getNthJuice(nthnode int) int { //get the nth node from the available nodes beginning at the lowest id node
	count := 0
	for i := 0; i < numOfServer; i++ {
		if JuiceNodes[i] == 1 {
			count = count + 1
		}
		if count == nthnode {
			return i
		}
	}
	return 0
}

//pass in name and it will hash to the nth node of choosen juice nodes
func juicehashfile(filename string, num_juices int) int {
	//make a hash
	h := fnv.New32a()
	h.Write([]byte(filename))
	hashed_val := (h.Sum32() % uint32(num_juices))
	fmt.Println(hashed_val)
	return int(hashed_val)
}

func getJuiceJobs(file_prefix string, num_juice_jobs int) {
	files, err := ioutil.ReadDir(".")
	if err != nil {
		fmt.Println(err)
	}
	file_prefix = file_prefix + "_"
	hash_val := 0
	vm_num := 0
	for _, f := range files {
		if len(f.Name())>len(file_prefix){
		if string(f.Name()[0:len(file_prefix)]) == file_prefix {
			hash_val = juicehashfile(f.Name(), num_juice_jobs)
			fmt.Println("hashed_val: ", hash_val)
			vm_num = getNthJuice(hash_val + 1)
			fmt.Println("vm num", vm_num)
			JuiceJobs[vm_num] = JuiceJobs[vm_num] + " " + f.Name()
			fmt.Println(f.Name())
		}
	}
}

}

func combine_files() {
	out, err := os.OpenFile(sdfs_dest_filename, os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatalln("failed to open outpout file:", err)
	}

	//copy in the masters file
	/*  fmt.Println("combining ",file_prefix+strconv.Itoa(Master)+".txt")
	    zipIn, err := os.Open(file_prefix+strconv.Itoa(Master)+".txt")
	    defer zipIn.Close()
	    _, err = io.Copy(out, zipIn)
	    defer out.Close()
	    check(err)
	    err = os.Remove(file_prefix+strconv.Itoa(Master)+".txt")
	    check(err)*/

	files, err := ioutil.ReadDir("storage")
	if err != nil {
		fmt.Println(err)
	}
	file_prefix := "reducer_intermediate_file"
	for _, f := range files {
		//fmt.Println("checking file",f.Name())

		if len(f.Name()) > len(file_prefix) {
			if string(f.Name()[0:len(file_prefix)]) == file_prefix {
				zipIn, err := os.Open("storage/" + f.Name())
				if err != nil {
					log.Fatalln("failed to open zip for reading:", err)
				}
				defer zipIn.Close()
				_, err = io.Copy(out, zipIn)
				//remove file
				// err = os.Remove("storage/"+f.Name())
				check(err)

			}
		}
	}

}
func PUTJuiceResults(outgoing_updates chan job) {
	//choose nodes to put file in
	//choose random number
	fmt.Println("sending the results")
	sendPUTupdates(outgoing_updates, sdfs_dest_filename, Master)

	aliveMutex.Lock()
	rand_num := rand.Intn(numOfServer-1) % aliveNode
	aliveMutex.Unlock()

	fmt.Println("choose random number to put results to node ", rand_num)
	num_of_PUTS := 1
	index := rand_num
	for i := 0; i < numOfServer; i++ {
		index = (index + 1) % numOfServer
		PresentMutex.Lock()
		if present[index] == 1 && index != Master {
			//PUT file here and send update
			PresentMutex.Unlock()
			fileSender(index, sdfs_dest_filename, "storage/"+sdfs_dest_filename, false, false)
			sendPUTupdates(outgoing_updates, sdfs_dest_filename, index)
			num_of_PUTS = num_of_PUTS + 1

		} else {
			PresentMutex.Unlock()
		}
		aliveMutex.Lock()
		if num_of_PUTS == 4 || num_of_PUTS == aliveNode {
			break
		}
		aliveMutex.Unlock()

	}

}
