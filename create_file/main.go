package main

import (
	"bufio"
	"fmt"
	"os"
	pipeline "wenxi/Network-Merge-Sorting/demo4/pipeline4"
)

func main() {
	var (
		file   *os.File
		err    error
		p      <-chan int
		writer *bufio.Writer
		reader *bufio.Reader
	)
	const (
		filename = "small.in"
		n        = 64
	)

	file, err = os.Create(filename)
	defer file.Close()

	if err != nil {
		fmt.Printf("Failed to create file : %s\n", err.Error())
		return
	}

	writer = bufio.NewWriter(file)
	pipeline.WriteSink(writer,
		pipeline.RandomSource(n))
	writer.Flush()

	file, err = os.Open(filename)
	defer file.Close()

	if err != nil {
		fmt.Printf("Failed to open file : %s\n", err.Error())
		return
	}

	reader = bufio.NewReader(file)
	p = pipeline.ReaderSource(reader, -1)

	count := 0
	for v := range p {
		fmt.Println(v)
		count++
		if count > 100 {
			break
		}
	}

}

func mergeDemo() {
	a := []int{3, 5, 96, 7, 52, 399, 4}
	b := []int{85, 455, 396, 27, 52, 399}
	p := pipeline.Merge(pipeline.InMemSort(pipeline.ArraySource(a)),
		pipeline.InMemSort(pipeline.ArraySource(b)))

	for v := range p {
		fmt.Println(v)
	}
}
