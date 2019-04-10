package pipeline4

import (
	"bufio"
	"fmt"
	"net"
)

func NetworkSink(addr string, in <-chan int) {
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		fmt.Printf("Failed to listen addr: %s, err: %s\n ",
			addr, err.Error())
		return
	}

	go func() {
		defer listener.Close()

		conn, err := listener.Accept()
		defer conn.Close()
		if err != nil {
			panic(err)
		}

		writer := bufio.NewWriter(conn)
		defer writer.Flush()
		WriteSink(writer, in)
	}()
}

func NetworkSource(addr string) <-chan int {
	out := make(chan int)
	go func() {
		conn, err := net.Dial("tcp", addr)
		defer conn.Close()
		if err != nil {
			panic(err)
		}

		r := ReaderSource(bufio.NewReader(conn), -1)

		for v := range r {
			out <- v
		}
		close(out)
	}()
	return out
}
