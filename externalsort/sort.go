package main

import (
	"bufio"
	"fmt"
	"os"
	"strconv"
	"wenxi/Network-Merge-Sorting/demo4/pipeline4"
)

const (
	inputFilename  = "test.in"
	outputFilename = "test.out"
	fileSize       = 80000000
	chunkCount     = 4
)

func main() {
	p := createNetworkPipeline(inputFilename, fileSize, chunkCount)
	writeToFile(p, outputFilename)
	printFile(outputFilename)
}

func printFile(filename string) {
	file, err := os.Open(filename)
	defer file.Close()
	if err != nil {
		fmt.Println("Failed to open " + filename)
		panic(err)
	}

	p := pipeline4.ReaderSource(file, -1)
	count := 0
	for v := range p {
		fmt.Println(v)
		count++
		if count > 100 {
			break
		}
	}
}

func writeToFile(p <-chan int, filename string) {
	file, err := os.Create(filename)
	defer file.Close()
	if err != nil {
		fmt.Println("Failed to create outputFilename")
		panic(err)
	}

	writer := bufio.NewWriter(file)
	defer writer.Flush()

	pipeline4.WriteSink(writer, p)
}

func createPipeline(filename string,
	fileSize, chunkCount int) <-chan int {

	pipeline4.Init()
	sortResults := []<-chan int{}
	chunkSize := fileSize / chunkCount
	for i := 0; i < chunkCount; i++ {
		file, err := os.Open(filename)
		if err != nil {
			fmt.Println("Failed to open " + filename)
			panic(err)
		}

		// 第一个参数表示从第几块开始，
		// 第二个参数0表示从头开始
		file.Seek(int64(i*chunkSize), 0)

		source := pipeline4.ReaderSource(bufio.NewReader(file), chunkSize)

		sortResults = append(sortResults, pipeline4.InMemSort(source))
	}

	return pipeline4.MergeN(sortResults...)
}

func createNetworkPipeline(filename string,
	fileSize, chunkCount int) <-chan int {

	pipeline4.Init()
	sortAddr := []string{}
	chunkSize := fileSize / chunkCount
	for i := 0; i < chunkCount; i++ {
		file, err := os.Open(filename)
		if err != nil {
			fmt.Println("Failed to open " + filename)
			panic(err)
		}

		// 第一个参数表示从第几块开始，
		// 第二个参数0表示从头开始
		file.Seek(int64(i*chunkSize), 0)

		source := pipeline4.ReaderSource(bufio.NewReader(file), chunkSize)

		addr := ":" + strconv.Itoa(7000+i)
		pipeline4.NetworkSink(addr, pipeline4.InMemSort(source))
		sortAddr = append(sortAddr, addr)
	}

	sortResults := []<-chan int{}
	for _, addr := range sortAddr {
		sortResults = append(sortResults, pipeline4.NetworkSource(addr))
	}

	return pipeline4.MergeN(sortResults...)
}
