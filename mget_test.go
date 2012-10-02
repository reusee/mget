package mget

import (
	"testing"
	"net"
	"fmt"
	iconv "github.com/reusee/go-iconv"
)

func TestMget(t *testing.T) {
  Verbose = true
	end := make(chan bool)
	contentChan := make(chan *Content)
	n := 50
	go func() {
		for i := 0; i < n; i++ {
			content := <-contentChan
			fmt.Printf("fetch %s, len %d\n", content.Url, len(content.Content))
		}
		end <- true
	}()
	go StartSocket(StartScheduler(contentChan, StartCollector()))
	conn, err := net.Dial("tcp", "localhost:43210")
	if err != nil {
		panic(err)
	}
	for i := 0; i < n; i++ {
		fmt.Fprintf(conn, "http://www.qq.com\n")
	}
	<-end
}

func TestChannelMget(t *testing.T) {
  Verbose = true
  mget := NewChannelMget()
  mget.Collector.SetHeader <- map[string]string{
    "Referer": "hehe",
  }
  n := 100
  for i := 0; i < n; i++ {
    mget.Request <- "http://www.qq.com"
  }
  for i := 0; i < n; i++ {
    res := <-mget.Response
    gbk2utf8(res.Content)
  }
}

func gbk2utf8(input []byte) ([]byte, error) {
	cd, _ := iconv.Open("utf8//IGNORE", "gbk")
  defer cd.Close()
	return cd.Conv(input)
}
