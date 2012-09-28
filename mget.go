package mget

import (
	"bufio"
	"fmt"
	"io/ioutil"
	"net"
	httpc "github.com/mreiferson/go-httpclient"
	"runtime"
	"strings"
	"time"
	"net/http"
)

var PoolSize = 30

func StartCollector() *Collector {
	Input := make(chan *CollectJob)
	SetCookieChan := make(chan string)
	cookie := make([]*http.Cookie, 0)
	setCookie := func(str string) {
		cookie = make([]*http.Cookie, 0)
		for _, part := range strings.Split(str, ";") {
			pair := strings.Split(part, "=")
			name, value := strings.TrimSpace(pair[0]), strings.TrimSpace(pair[1])
			cookie = append(cookie, &http.Cookie{Name: name, Value: value})
		}
	}
	go func() {
		for {
			select {
			case job := <-Input:
				go func() {
					content := collect(job.url, cookie)
					job.ret <- content
				}()
			case cookieStr := <-SetCookieChan:
				setCookie(cookieStr)
			}
		}
	}()
	return &Collector{Input, SetCookieChan}
}

type Collector struct {
	Input chan *CollectJob
	SetCookie chan string
}

type CollectJob struct {
	url string
	ret chan []byte
}

func collect(url string, cookies []*http.Cookie) []byte {
	client := httpc.New()
	client.ConnectTimeout = time.Second * 30
	client.ReadWriteTimeout = time.Second * 30
	retry := 5
	request, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return []byte("")
	}
	for _, cookie := range cookies {
		request.AddCookie(cookie)
	}
start: resp, err := client.Do(request)
	if resp != nil {
		defer resp.Body.Close()
	}
	if err != nil {
		if retry > 0 {
			retry--
			goto start
		}
		return []byte("")
	}
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		if retry > 0 {
			retry--
			goto start
		}
		return []byte("")
	}
	return body
}

type Content struct {
	Url     string
	Content []byte
}

func StartStorage() (StoreChan chan *Content) {
	StoreChan = make(chan *Content)
	go func() {
		for {
			content := <-StoreChan
			fmt.Printf("received: %s, length %d\n",
				content.Url,
				len(content.Content))
		}
	}()
	return StoreChan
}

func StartScheduler(storage chan *Content, collector *Collector) (SchedChan chan string) {
	SchedChan = make(chan string)
	go func() {
		jobs := make([]string, 0)
		semaphore := make(chan bool, PoolSize)
		for i := 0; i < PoolSize; i++ {
			semaphore <- true
		}
		var url string
		var total, done, delta uint
		var sysmem uint64
		doneChan := make(chan bool)
		ticker := time.NewTicker(time.Second * 1)
		memstats := new(runtime.MemStats)
		addJob := func(url string) {
			fmt.Printf("add: %s\n", url)
			jobs = append(jobs, url)
			total++
		}
		doneJob := func() {
			done++
		}
		doJob := func() {
			url, jobs = jobs[len(jobs)-1], jobs[:len(jobs)-1]
			go func(url string) {
				defer func() {
					semaphore <- true
					doneChan <- true
				}()
				ret := make(chan []byte)
				collector.Input <- &CollectJob{url, ret}
				content := <-ret
				storage <- &Content{url, content}
			}(url)
		}
		showStat := func() {
			runtime.ReadMemStats(memstats)
			sysmem = memstats.Sys
			fmt.Printf("done %d / %d, pending %d, delta %d, mem %dm\n",
				done, total, total-done, done-delta, sysmem/1024/1024)
			delta = done
		}
		for {
			if len(jobs) > 0 {
				select {
				case url = <-SchedChan:
					addJob(url)
				case <-doneChan:
					doneJob()
				case <-semaphore:
					doJob()
				case <-ticker.C:
					showStat()
				}
			} else {
				select {
				case url = <-SchedChan:
					addJob(url)
				case <-doneChan:
					doneJob()
				case <-ticker.C:
					showStat()
				}
			}
		}
	}()
	return SchedChan
}

func StartSocket(schedChan chan string) {
	ln, err := net.Listen("tcp", ":43210")
	if err != nil {
		panic("Listen error")
	}
	for {
		conn, err := ln.Accept()
		if err != nil {
			continue
		}
		go func(conn net.Conn) {
			defer conn.Close()
			reader := bufio.NewReader(conn)
			for {
				line, err := reader.ReadString('\n')
				if err != nil {
					return
				}
				line = strings.TrimSpace(line)
				schedChan <- line
			}
		}(conn)
	}
}

func StartChan(schedChan chan string) chan string {
	Input := make(chan string)
	go func() {
		var url string
		for {
			url = <-Input
			url = strings.TrimSpace(url)
			schedChan <- url
		}
	}()
	return Input
}

func NewChannelMget() *ChannelMget {
  collector := StartCollector()
  response := make(chan *Content, 2048)
  request := StartChan(StartScheduler(response, collector))
  return &ChannelMget{
    Collector: collector,
    Request: request,
    Response: response,
  }
}

type ChannelMget struct {
  Collector *Collector
  Request chan string
  Response chan *Content
}
