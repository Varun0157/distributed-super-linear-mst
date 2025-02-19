package main

import (
	"fmt"
	"log"
	"math/rand"
	utils "mst/superlinear/utils"
	"time"

	"golang.org/x/sync/errgroup"
)

func (s *FilteringServer) partitionMSF(msf []utils.Edge) error {
	numNodes := s.md.numComputationalNodes(s.round)

	edges := msf
	rand.Shuffle(len(edges), func(i, j int) { edges[i], edges[j] = edges[j], edges[i] })
	partitionedEdges, err := utils.Partition(edges, numNodes)
	if err != nil {
		return fmt.Errorf("failed to partition msf: %v", err)
	}

	// the nodes to send to are the first numNodes nodes in the list
	nodesToSend := s.addrDetails.nodes[:numNodes]

	var eg errgroup.Group
	for i, addr := range nodesToSend {
		i, addr := i, addr // capture in local scope
		eg.Go(func() error {
			log.Printf("%s(%d) - sending %d edges to %s", s.addrDetails.nodes[s.addrDetails.index], s.round, len(partitionedEdges[i]), addr)

			// "sending" to self
			if i == s.addrDetails.index {
				s.addEdges(partitionedEdges[i], s.addrDetails.nodes[s.addrDetails.index])
				return nil
			}

			// sending to neighbouring node
			if err := sendEdges(partitionedEdges[i], s.addrDetails.nodes[s.addrDetails.index], addr); err != nil {
				return fmt.Errorf("%s - failed to send edges to %s: %v", s.addrDetails.nodes[s.addrDetails.index], addr, err)
			}

			return nil
		})
	}

	if err := eg.Wait(); err != nil {
		return fmt.Errorf("failed to send edges: %v", err)
	}

	return nil
}

func (s *FilteringServer) progressRound(edges []utils.Edge) {
	s.round += 1
	log.Printf("%s - partitioning MST for round %d", s.addrDetails.nodes[s.addrDetails.index], s.round)
	err := s.partitionMSF(edges)
	if err != nil {
		log.Fatalf("%s - failed to partition MST: %v", s.addrDetails.nodes[s.addrDetails.index], err)
	}
}

func (s *FilteringServer) runDriver(outFile string) {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		if s.md.numComputationalNodes(s.round) <= s.addrDetails.index {
			s.ShutDown()
			return
		}

		enoughReceipts, err := s.edgeData.receivedAllEdges(s.round, s.md)
		if err != nil {
			log.Fatalf("%s - failed to check if all edges received: %v", s.addrDetails.nodes[s.addrDetails.index], err)
		}
		if !enoughReceipts {
			continue
		}

		msf, err := s.edgeData.MSF(s.round)
		if err != nil {
			log.Fatalf("%s - unable to calculate MST: %v", s.addrDetails.nodes[s.addrDetails.index], err)
		}
		log.Printf("%s - mst calculated: %d edges\n", s.addrDetails.nodes[s.addrDetails.index], len(msf))

		// only one node in current round => last round
		if s.md.numComputationalNodes(s.round) <= 1 {
			err := utils.WriteGraph(outFile, msf)
			if err != nil {
				log.Fatalf("%s - failed to write graph: %v", s.addrDetails.nodes[s.addrDetails.index], err)
			}
			s.ShutDown()
			log.Printf("COMPUTATION COMPLETE in %d rounds", s.round)
			return
		}

		s.progressRound(msf)
	}
}
