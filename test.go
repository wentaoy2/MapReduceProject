package main

import(
"fmt"
"io/ioutil"


)

func main(){


    files, err := ioutil.ReadDir("storage")
    if err != nil {
        fmt.Println(err)
    }

    for _, f := range files {
      fmt.Println(f.Name())
    }
}
