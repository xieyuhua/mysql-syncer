package main

import (
	"flag"
	"os"
// 	"time"
	"os/signal"
	"runtime"
	"syscall"
	"github.com/juju/errors"
	"github.com/siddontang/go-log/log"
	"github.com/zhaochuanyun/go-mysql-syncer/river"
)


var configFile = flag.String("config", "./etc/river.toml", "mysqlsync config file")
var data_dir = flag.String("data_dir", "", "path for mysqlsync to save data")
var server_id = flag.Int("server_id", 0, "MySQL server id, as a pseudo slave")
var flavor = flag.String("flavor", "mysql", "flavor: mysql or mariadb")
var execution = flag.String("exec", "", "mysqldump execution path")
var logLevel = flag.String("log_level", "info", "log level")
var thread = flag.Int("thread", 1, "The client connect thread num")


func main() {
    
	runtime.GOMAXPROCS(runtime.NumCPU())
	flag.Parse()

	log.SetLevelByName(*logLevel)

	sc := make(chan os.Signal, 1)
	signal.Notify(sc,
		os.Kill,
		os.Interrupt,
		syscall.SIGHUP,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT)

	cfg, err := river.NewConfigWithFile(*configFile)
	if err != nil {
		println(errors.ErrorStack(err))
		return
	}

	if *server_id > 0 {
		cfg.ServerID = uint32(*server_id)
	}

	if len(*data_dir) > 0 {
		cfg.DataDir = *data_dir
	}

	if len(*flavor) > 0 {
		cfg.Flavor = *flavor
	}

	if len(*execution) > 0 {
		cfg.DumpExec = *execution
	}

	if *thread > 1 {
		cfg.Thread = *thread
	}

	r, err := river.NewRiver(cfg)
	if err != nil {
		println(errors.ErrorStack(err))
		return
	}

//     donsae := make(chan struct{}, 1)
// 	go func() {
//         var chans = make(chan bool, 5)
//         i := 1
//         for{
//             i++
//             chans <- true
//             go func(i int) {
//                 log.Infof("**************************** --> %d", i)
//                 time.Sleep(4 * time.Second) 
//         		 <-chans
//             }(i)
//         }
// 		donsae <- struct{}{}
// 	}()

    

	done := make(chan struct{}, 1)
	go func() {
		r.Run()
		done <- struct{}{}
	}()

	select {
	case n := <-sc:
		log.Infof("receive signal %v, closing", n)
	case <-r.Ctx().Done():
		log.Infof("context is done with %v, closing", r.Ctx().Err())
	}

	r.Close()
	<-done
}
