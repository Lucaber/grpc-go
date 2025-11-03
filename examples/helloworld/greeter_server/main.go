/*
 *
 * Copyright 2015 gRPC authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

// Package main implements a server for Greeter service.
package main

import (
	"flag"
	"fmt"
	"iter"
	"log"
	"net"
	"os"
	"runtime"
	"runtime/pprof"
	"strconv"

	"google.golang.org/grpc"
	pb "google.golang.org/grpc/examples/helloworld/helloworld"
)

var (
	port = flag.Int("port", 50051, "The server port")
)

// server is used to implement helloworld.GreeterServer.
type server struct {
	pb.UnimplementedGreeterServer
}

func (s *server) StreamHelloIterator(req *pb.HelloRequest, se grpc.ServerStreamingServer[pb.HelloReply]) error {
	f, err := os.Create("server.pprof")
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()
	err = pprof.StartCPUProfile(f)
	if err != nil {
		panic(err)
	}
	defer pprof.StopCPUProfile()
	done := se.Context().Done()
	i := 0

	var items iter.Seq[int] = func(yield func(i int) bool) {
		for {
			if !yield(i) {
				return
			}
			i++
		}
	}

	res := &pb.HelloReply{Message: strconv.Itoa(i)}

	for i := range items {
		select {
		case <-done:
			return nil
		default:
			res.Message = strconv.Itoa(i)
			se.SendMsg(res)
		}
	}
	return nil
}

func (s *server) StreamHelloChannel(req *pb.HelloRequest, se grpc.ServerStreamingServer[pb.HelloReply]) error {
	f, err := os.Create("server.pprof")
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()
	err = pprof.StartCPUProfile(f)
	if err != nil {
		panic(err)
	}
	defer pprof.StopCPUProfile()
	done := se.Context().Done()
	i := 0

	items := make(chan int, 10000)

	go func() {
		for {
			select {
			case <-done:
				return
			default:
				items <- i
				i++
			}
		}
	}()

	for i := range items {
		select {
		case <-done:
			return nil
		default:

			se.SendMsg(&pb.HelloReply{Message: "Hello " + req.GetName() + " " + strconv.Itoa(i)})
			i++
		}
	}
	return nil
}

func (s *server) StreamHelloDirect(req *pb.HelloRequest, se grpc.ServerStreamingServer[pb.HelloReply]) error {
	f, err := os.Create("server.pprof")
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()
	err = pprof.StartCPUProfile(f)
	if err != nil {
		panic(err)
	}
	defer pprof.StopCPUProfile()
	done := se.Context().Done()
	i := 0
	for {
		select {
		case <-done:
			return nil
		default:
			se.SendMsg(&pb.HelloReply{Message: "Hello " + req.GetName() + " " + strconv.Itoa(i)})
			i++
		}
	}
}

func (s *server) StreamHelloDirectSched(req *pb.HelloRequest, se grpc.ServerStreamingServer[pb.HelloReply]) error {
	f, err := os.Create("server.pprof")
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()
	err = pprof.StartCPUProfile(f)
	if err != nil {
		panic(err)
	}
	defer pprof.StopCPUProfile()
	done := se.Context().Done()
	i := 0
	for {
		select {
		case <-done:
			return nil
		default:
			se.SendMsg(&pb.HelloReply{Message: "Hello " + req.GetName() + " " + strconv.Itoa(i)})
			i++
			// needed to reproduce syscall cpu usage
			runtime.Gosched()
		}
	}
}

func main() {
	flag.Parse()
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", *port))
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	s := grpc.NewServer()
	pb.RegisterGreeterServer(s, &server{})
	log.Printf("server listening at %v", lis.Addr())
	if err := s.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
