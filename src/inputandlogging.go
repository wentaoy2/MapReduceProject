package main

import (
	"bufio"
	"fmt"
	"os"
	"time"
)

/*
This handles user input.
You can type in "Self", "List",
and "Leave" to print out self id,
the membership list, and leaving respectively
*/
func handleStdin() {
	start_stdin := time.Now()
	fmt.Println("entered stdin: ")

	reader := bufio.NewReader(os.Stdin)
	for {

		fmt.Print("Command: ")
		text, _ := reader.ReadString('\n')
		fmt.Println("entered command:", text)

		if text == "List\n" {
			fmt.Println("ACTIVE MACHINE IPS: ")

			PresentMutex.Lock()
			for i := 0; i < numOfServer; i++ {
				if present[i] == 1 {
					fmt.Println(Addr[i])
				}
			}
			PresentMutex.Unlock()

		} else if text == "Self\n" {
			elapsed := time.Since(start_stdin)

			fmt.Println("machine time: ", elapsed)
			fmt.Println("machine id: ", jsonId)
			fmt.Println("machine ip: ", Addr[jsonId])

		} else if text == "Leave\n" {

			fmt.Println("NODE is VOLUNTARILY LEAVING")
			LeaveMutex.Lock()
			leave = true
			LeaveMutex.Unlock()
			break
		}

	}
	return

}

/*
The rest of the functions handle writing
to the log file
*/
func isError(err error) bool {
	if err != nil {
		fmt.Println(err.Error())
	}

	return (err != nil)
}
func createFile(path string) {
	// detect if file exists
	var _, err = os.Stat(path)

	// create file if not exists
	if os.IsNotExist(err) {
		var file, err = os.Create(path)
		if isError(err) {
			return
		}
		defer file.Close()
	}

	fmt.Println("==> done creating file", path)
}
func writeFile(text string) {

	var err error
	// write some text line-by-line to file
	_, err = file.WriteString(text)
	if isError(err) {
		return
	}

	// save changes
	err = file.Sync()
	if isError(err) {
		return
	}
}
