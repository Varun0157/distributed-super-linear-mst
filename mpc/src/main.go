package main

import (
	"fmt"
	"log"
	"math/rand"
	"net"
	"os"
	"strconv"
	"sync"
	"time"

	utils "mst/superlinear/utils"
)

func listenOnRandomAddr() (lis net.Listener, err error) {
	log.Println("attempting to listen on random port")
	for {
		port := rand.Intn(65535-1024) + 1024
		addr := fmt.Sprintf(":%d", port)

		lis, err = net.Listen("tcp", addr)
		if err == nil {
			break
		}

		log.Printf("failed to listen on addr %s: %v", addr, err)
	}
	log.Printf("listening on port %s", lis.Addr().String())

	return lis, nil
}

func run(graphFile string, outFile string, epsilon float64) error {
	edges, err := utils.ReadGraph(graphFile)
	if err != nil {
		return err
	}

	numVertices, err := utils.GetNumberOfVertices(edges)
	if err != nil {
		return err
	}

	md := NewMetaData(len(edges), numVertices, epsilon)
	log.Printf("metadata: %v", md)
	md.printRoundDetails()
	numComputationalNodes := md.numComputationalNodes(0)

	partitionedEdges, err := utils.Partition(edges, numComputationalNodes)
	if err != nil {
		return fmt.Errorf("failed to partition edges: %v", err)
	}

	listeners := make([]net.Listener, 0)
	for range numComputationalNodes {
		lis, err := listenOnRandomAddr()
		if err != nil {
			return fmt.Errorf("failed to listen on port: %v", err)
		}

		listeners = append(listeners, lis)
	}

	nodes := make([]string, 0)
	for _, nodeLis := range listeners {
		nodes = append(nodes, nodeLis.Addr().String())
	}

	startTime := time.Now()

	serverWg := sync.WaitGroup{}
	for i := range numComputationalNodes {
		serverWg.Add(1)

		// shutdown the server and make it go out of scope when the driver returns
		go func() {
			defer serverWg.Done()

			localEdges := partitionedEdges[i]
			localLis := listeners[i]

			s, err := NewFilteringServer(localLis, nodes, localEdges, md)
			if err != nil {
				log.Fatalf("failed to create server: %v", err)
			}

			s.runDriver(outFile)
			s.ShutDown()
			log.Printf("COMPUTATION COMPLETE -> %d rounds", s.round)
		}()
	}
	serverWg.Wait()

	elapsedTime := time.Since(startTime)
	log.Printf("elapsed time: %v", elapsedTime)

	return nil
}

func main() {
	if len(os.Args) != 4 {
		fmt.Println("usage: go run *.go <infile> <outfile> <epsilon>")
		os.Exit(1)
	}

	epsilon, err := strconv.ParseFloat(os.Args[3], 64)
	if err != nil {
		log.Fatalf("error parsing epsilon: %v", err)
	}
	run(os.Args[1], os.Args[2], epsilon)
}
