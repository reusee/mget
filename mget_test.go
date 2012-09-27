package mget

import (
	"testing"
	"net"
	"fmt"
)

func TestMget(t *testing.T) {
	end := make(chan bool)
	contentChan := make(chan *Content)
	n := 50
	go func() {
		for i := 0; i < n; i++ {
			content := <-contentChan
			fmt.Printf("fetch %s, len %d\n", content.url, len(content.content))
		}
		end <- true
	}()
	go StartSocket(StartScheduler(contentChan))
	conn, err := net.Dial("tcp", "localhost:43210")
	if err != nil {
		panic(err)
	}
	for i := 0; i < n; i++ {
		fmt.Fprintf(conn, "http://www.qq.com\n")
	}
	<-end
}
