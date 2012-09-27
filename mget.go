package mget

import (
	"bufio"
	"fmt"
	"io/ioutil"
	"net"
	http "github.com/mreiferson/go-httpclient"
	"runtime"
	"strings"
	"time"
)

var PoolSize = 30

func collect(url string) []byte {
	client := http.New()
	client.ConnectTimeout = time.Second * 30
	client.ReadWriteTimeout = time.Second * 30
	retry := 5
start: resp, err := client.Get(url)
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
	url     string
	content []byte
}

func StartStorage() (storeChan chan *Content) {
	storeChan = make(chan *Content)
	go func() {
		for {
			content := <-storeChan
			fmt.Printf("received: %s, length %d\n",
				content.url,
				len(content.content))
		}
	}()
	return storeChan
}

func StartScheduler(storage chan *Content) (schedChan chan string) {
	schedChan = make(chan string)
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
				content := collect(url)
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
				case url = <-schedChan:
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
				case url = <-schedChan:
					addJob(url)
				case <-doneChan:
					doneJob()
				case <-ticker.C:
					showStat()
				}
			}
		}
	}()
	return schedChan
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
	input := make(chan string)
	go func() {
		var url string
		for {
			url = <-input
			url = strings.TrimSpace(url)
			schedChan <- url
		}
	}()
	return input
}
