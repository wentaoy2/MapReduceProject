package main

import (
	"bufio"
	"fmt"
	"os"
	"strings"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Println("Missing parameter, provide file name!")
		return
	}
	file, err := os.Open(os.Args[1])
	if err != nil {
		fmt.Println("Can't read file:", os.Args[1])
		panic(err)
	}
	fscanner := bufio.NewScanner(file)

	temp := ""
	count := 0
	//one := strconv.Itoa(1)
	for fscanner.Scan() {
		if count == 10 {
			s := strings.Fields(temp)

			for i := 0; i < len(s); i += 2 {
				fmt.Println(s[i+1] + " " + s[i])
			}
			count = 0
			temp = ""
		}
		count++
		temp += fscanner.Text()

	}
	s := strings.Fields(temp)
	for i := 0; i < len(s); i += 2 {
		fmt.Println(s[i+1] + " " + s[i])
	}
}
