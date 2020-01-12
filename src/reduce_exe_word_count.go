package main

import (
	"fmt"
	"os"
	"bufio"
)


func main() {
	file_name := os.Args[1]
    file, err := os.Open(file_name)
    defer file.Close()

    if err != nil {
        return
    }

    // Start reading from the file with a reader.
    reader := bufio.NewReader(file)
		count:= 0
    var line string
    for {
        line, err = reader.ReadString('\n')
				if line != ""{
				count = count+1
			}
        // Process the line here.
        if err != nil {
            break
        }
    }

fmt.Println(count)

    return
}
