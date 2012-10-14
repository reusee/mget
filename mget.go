package mget

import (
	"bufio"
	"fmt"
	"io/ioutil"
	"net"
	"runtime"
	"strings"
	"time"
	"net/http"
)

var (
  PoolSize = 30
  CacheSize = 30
  Timeout = time.Second * 10
  Retry = 5
  Verbose = false
)

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
  SetHeaderChan := make(chan map[string]string)
  headers := make(map[string]string)
  setHeader := func(hs map[string]string) {
    for k, v := range hs {
      headers[k] = v
    }
  }
  transport := &http.Transport {
    Dial: func(netw, addr string) (conn net.Conn, err error) {
      conn, err = net.Dial(netw, addr)
      if conn != nil {
        conn.SetDeadline(time.Now().Add(Timeout))
        conn.SetReadDeadline(time.Now().Add(Timeout).Add(Timeout))
      } else {
        fmt.Printf("dial error%v\n", err)
      }
      return conn, err
    },
  }
	go func() {
		for {
			select {
			case job := <-Input:
        verbose(fmt.Sprintf("Collector new job %s", job.url))
				go func() {
					content, err := collect(job.url, cookie, headers, transport)
					job.ret <- &JobReturn{content, err}
				}()
			case cookieStr := <-SetCookieChan:
				setCookie(cookieStr)
      case headers := <-SetHeaderChan:
        setHeader(headers)
			}
		}
	}()
	return &Collector{Input, SetCookieChan, SetHeaderChan}
}

type Collector struct {
	Input chan *CollectJob
	SetCookie chan string
	SetHeader chan map[string]string
}

type JobReturn struct {
  content []byte
  err error
}

type CollectJob struct {
	url string
	ret chan *JobReturn
}

func collect(url string, cookies []*http.Cookie, headers map[string]string, transport *http.Transport) ([]byte, error) {
  defer transport.CloseIdleConnections()
	retry := Retry
	request, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return []byte(""), err
	}
	for _, cookie := range cookies {
		request.AddCookie(cookie)
	}
  for k, v := range headers {
    request.Header.Add(k, v)
  }
start:
  client := http.Client {
    Transport: transport,
  }
  resp, err := client.Do(request)
  if resp != nil {
    defer resp.Body.Close()
  }
	if err != nil {
		if retry > 0 {
		  verbose(fmt.Sprintf("Collector request retry %d %s", retry, url))
			retry--
			goto start
		}
		verbose(fmt.Sprintf("Collector empty response %s", url))
		return []byte(""), err
	}
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		if retry > 0 {
		  verbose(fmt.Sprintf("Collector read retry %d %s", retry, url))
			retry--
			goto start
		}
		verbose(fmt.Sprintf("Collector empty read %s", url))
		return []byte(""), err
	}
	return body, err
}

type Content struct {
	Url     string
	Content []byte
	Err error
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
		doneChan := make(chan string)
		ticker := time.NewTicker(time.Second * 1)
		memstats := new(runtime.MemStats)
		addJob := func(url string) {
			jobs = append(jobs, url)
			total++
			//verbose(fmt.Sprintf("Scheduler add job %s", url))
		}
		doneJob := func(url string) {
			done++
			verbose(fmt.Sprintf("Scheduler done job %s", url))
		}
		doJob := func() {
			url, jobs = jobs[len(jobs)-1], jobs[:len(jobs)-1]
			verbose(fmt.Sprintf("Scheduler start job %s", url))
			go func(url string) {
				defer func() {
					semaphore <- true
					doneChan <- url
				}()
				ret := make(chan *JobReturn)
				collector.Input <- &CollectJob{url, ret}
				jobRet := <-ret
				storage <- &Content{url, jobRet.content, jobRet.err}
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
				case url = <-doneChan:
					doneJob(url)
				case <-semaphore:
					doJob()
				case <-ticker.C:
					showStat()
				}
			} else {
				select {
				case url = <-SchedChan:
					addJob(url)
				case url = <-doneChan:
					doneJob(url)
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
  response := make(chan *Content, CacheSize)
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

func verbose(s string) {
  if Verbose {
    fmt.Printf("======== Verbose ======== %s\n", s)
  }
}
