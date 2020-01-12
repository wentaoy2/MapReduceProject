package main

import (
	"sync"
)

var writeToFileStoreMutex = &sync.Mutex{}
var writeToFileTempMutex = &sync.Mutex{}
var mapleMasterMutex = &sync.Mutex{}

var mapleRecorderMutex = &sync.Mutex{}
var mapleRecorder []int
var mapleIntermediateFiles map[string]int

//var mapleFileName string
var mapleTask []string
var mapleTaskFiles [][]string

func init_maple() {
	mapleRecorder = make([]int, 10)
	mapleTask = make([]string, 10)
	mapleTaskFiles = make([][]string, 10)
	mapleIntermediateFiles = make(map[string]int)
}
