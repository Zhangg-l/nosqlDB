package main

import (
	"go_code/project13/rosedb"
	"go_code/project13/rosedb/cmd/server/service"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"

	"google.golang.org/grpc"
)

// 服务器端初始化

func main() {
	s := grpc.NewServer()
	// init rosedb
	cnf := rosedb.DefaultConfig()
	db, err := rosedb.Open(*cnf)
	if err != nil {
		log.Fatalf("rosedb.Open err :%v", err)
	}
	grpcs := service.NewGrpcServer(db)
	service.RegisterRosedbServer(s, grpcs)

	lis, err := net.Listen("tcp", cnf.GrpcAddr)
	if err != nil {
		log.Fatalf(" net.Listen err :%v", err)
	}
	go func() {
		log.Println("server startup...")
		err = s.Serve(lis)
		if err != nil {
			log.Fatalf("s.Serve err :%v", err)
		}

	}()

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, os.Kill, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)

	<-sig
	grpcs.GrpcClose()
	log.Println("rosedb server closing")
}
