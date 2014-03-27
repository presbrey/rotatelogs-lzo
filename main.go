package main

import (
	"bufio"
	"bytes"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	_path "path"
	"runtime"
	"strings"
	"syscall"
	"time"

	_strftime "bitbucket.org/tebeka/strftime"
	"github.com/cyberdelia/lzo"
)

var (
	logFile      = flag.String("logFile", "%Y/%m/%d/%s_%h.log.lzo", "")
	rotationTime = flag.Duration("rotationTime", time.Minute, "")

	backlog      = flag.Int("backlog", 65536, "")
	blockSizeMax = flag.Int("blockSizeMax", 256*1024, "")
	level        = flag.Int("level", lzo.DefaultCompression, "compression level")
	lockMode     = flag.Int("lockMode", syscall.LOCK_EX, "")
	tcp          = flag.String("tcp", ":9630", "")

	hostname = ""
)

func strftime() (string, error) {
	var err error
	now := time.Now()
	path := strings.Replace(*logFile, "%s", fmt.Sprint(now.Unix()), -1)
	path = strings.Replace(path, "%h", hostname, -1)
	path, err = _strftime.Format(path, now)
	if err != nil {
		return "", err
	}
	return path, nil
}

func init() {
	var err error
	hostname, err = os.Hostname()

	flag.Parse()

	_, err = strftime()
	if err != nil {
		log.Fatalln(err)
	}

	if *level >= lzo.BestCompression {
		*level = lzo.BestCompression
	} else if *level <= lzo.BestSpeed {
		*level = lzo.BestSpeed
	} else {
		*level = lzo.DefaultCompression
	}

	if os.Getenv("GOMAXPROCS") == "" {
		runtime.GOMAXPROCS(runtime.NumCPU())
	}
}

func startTCP(ch chan<- net.Conn) error {
	s, err := net.Listen("tcp", *tcp)
	if err != nil {
		return err
	}

	go func() {
		for {
			if conn, err := s.Accept(); err == nil {
				ch <- conn
			} else {
				log.Println(err)
			}
		}
	}()
	return nil
}

func main() {
	var (
		buf       bytes.Buffer
		file      *os.File
		lzoWriter *lzo.Writer

		conns = make(chan net.Conn, 128)
		lines = make(chan string, *backlog)
		path  = ""
	)

	err := startTCP(conns)
	if err != nil {
		log.Fatalln(err)
	}

	reopen := func() {
		pathNew, _ := strftime()
		if pathNew == path {
			return
		}

		if buf.Len() > 0 {
			if _, err := buf.WriteTo(lzoWriter); err != nil {
				log.Println(err)
			}
		}

		os.MkdirAll(_path.Dir(pathNew), 0777)
		output, err := os.Create(pathNew)
		if err != nil {
			log.Println(err)
			return
		}
		if *lockMode > 0 {
			err = syscall.Flock(int(output.Fd()), *lockMode)
			if err != nil {
				log.Println(err)
				return
			}
		}
		compressor, err := lzo.NewWriterLevel(output, *level)
		if err != nil {
			log.Println(err)
			return
		}

		l0, f0 := lzoWriter, file
		lzoWriter, file = compressor, output

		if l0 != nil {
			l0.Close()
		}
		if f0 != nil {
			f0.Close()
		}
		path = pathNew
	}
	reopen()

	alive := true
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt)
	ticker := time.Tick(*rotationTime)
	for alive {
		select {
		case <-sigCh:
			alive = false

		case <-ticker:
			reopen()

		case c := <-conns:
			go func() {
				scanner := bufio.NewScanner(c)
				for scanner.Scan() {
					text := scanner.Text()
					if len(text) > 0 {
						lines <- text + "\n"
					}
				}
				c.Close()
			}()

		case line := <-lines:
			if buf.Len()+len(line) > *blockSizeMax {
				if _, err := buf.WriteTo(lzoWriter); err != nil {
					log.Println(err)
				}
			}
			if _, err := buf.WriteString(line); err != nil {
				log.Println(err)
			}

		} // select
	} // for

	if buf.Len() > 0 {
		buf.WriteTo(lzoWriter)
	}
	if lzoWriter != nil {
		lzoWriter.Close()
	}
	if file != nil {
		file.Close()
	}
}
