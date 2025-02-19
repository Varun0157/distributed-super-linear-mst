package main

import (
	"fmt"
	"log"
	"math"
	"sync"

	utils "mst/superlinear/utils"
)

type MetaData struct {
	totalEdges    int
	totalVertices int
	epsilon       float64

	numEdgesCache map[int]int
}

func NewMetaData(totalEdges, totalVertices int, epsilon float64) *MetaData {
	return &MetaData{
		totalEdges:    totalEdges,
		totalVertices: totalVertices,
		epsilon:       epsilon,
		numEdgesCache: make(map[int]int),
	}
}

func (md MetaData) String() string {
	return fmt.Sprintf("{totalEdges: %d, totalVertices: %d, epsilon: %f}", md.totalEdges, md.totalVertices, md.epsilon)
}

func (md *MetaData) S() float64 {
	return math.Pow(float64(md.totalVertices), float64(1+md.epsilon))
}

func (md *MetaData) numEdges(round int) int {
	if edges, ok := md.numEdgesCache[round]; ok {
		return edges
	}

	edges := md.totalEdges
	if round > 0 {
		edges = int(math.Ceil(float64(md.numEdges(round-1)) / math.Pow(float64(md.totalVertices), float64(md.epsilon))))
	}
	md.numEdgesCache[round] = edges

	return md.numEdgesCache[round]
}

func (md MetaData) numComputationalNodes(round int) int {
	return int(math.Ceil(float64(md.numEdges(round)) / md.S()))
}

func (md *MetaData) printRoundDetails() {
	log.Println("edges and nodes until completion:")
	round := 0
	for {
		numEdges := md.numEdges(round)
		numNodes := md.numComputationalNodes(round)
		log.Printf("-> round %d: %d edges, %d computational nodes", round, numEdges, numNodes)
		if numNodes == 1 {
			break
		}
		round += 1
	}
}

type ReceivedEdgeData struct {
	edgesMutex sync.Mutex
	edges      map[string][]([]utils.Edge)
}

func NewReceivedEdgeData() *ReceivedEdgeData {
	return &ReceivedEdgeData{
		edges: make(map[string][]([]utils.Edge)),
	}
}

func (R *ReceivedEdgeData) receivedAllEdges(round int, md *MetaData) (bool, error) {
	getNumPortsSent := func() int {
		R.edgesMutex.Lock()
		defer R.edgesMutex.Unlock()

		numPortsSent := 0
		for _, edges := range R.edges {
			if len(edges) < 1 {
				continue
			}
			numPortsSent += 1
		}
		return numPortsSent
	}

	numPortsSent := getNumPortsSent()
	expectedNodes := md.numComputationalNodes(round - 1)

	if numPortsSent > expectedNodes {
		log.Printf("[WARNING] received edges from %d nodes but expected %d", numPortsSent, expectedNodes)
	}
	return numPortsSent >= expectedNodes, nil
}

func (R *ReceivedEdgeData) MSF(round int) ([]utils.Edge, error) {
	getEdges := func() []utils.Edge {
		R.edgesMutex.Lock()
		defer R.edgesMutex.Unlock()

		// find the edges that are required for the current round
		edges := make([]utils.Edge, 0)
		for _, edgeList := range R.edges {
			if len(edgeList) < 1 {
				continue
			}
			edges = append(edges, edgeList[0]...)
		}

		// remove these edges from memory
		for i := range R.edges {
			if len(R.edges[i]) < 1 {
				continue
			}
			R.edges[i] = R.edges[i][1:]
		}

		return edges
	}

	edges := getEdges()
	return utils.MST(edges)
}

func (R *ReceivedEdgeData) addEdges(edges []utils.Edge, addr string) error {
	R.edgesMutex.Lock()
	defer R.edgesMutex.Unlock()

	R.edges[addr] = append(R.edges[addr], edges)
	return nil
}
