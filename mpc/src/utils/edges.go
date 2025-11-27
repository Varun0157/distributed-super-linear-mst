package utils

import (
	"bufio"
	"fmt"
	"math"
	"os"
	"sort"
)

type Edge struct {
	Src    int
	Dest   int
	Weight int
}

func NewEdge(src, dest, weight int) Edge {
	return Edge{
		Src:    src,
		Dest:   dest,
		Weight: weight,
	}
}

func getMaxVertex(edges []Edge) (int, error) {
	if len(edges) < 1 {
		return 0, fmt.Errorf("no edges provided")
	}

	maxVertex := 0
	for _, edge := range edges {
		maxVertex = int(math.Max(float64(maxVertex), float64(edge.Src)))
		maxVertex = int(math.Max(float64(maxVertex), float64(edge.Dest)))
	}

	return maxVertex, nil
}

func MST(edges []Edge) ([]Edge, error) {
	maxVertex, err := getMaxVertex(edges)
	if err != nil {
		return nil, fmt.Errorf("unable to get max vertex: %v", err)
	}

	k, err := NewKruskals(edges, maxVertex)
	if err != nil {
		return nil, fmt.Errorf("unable to create Kruskals: %v", err)
	}

	return k.ConstructMST()
}

func SortEdges(edges []Edge) {
	sort.Slice(edges, func(i, j int) bool {
		return (edges[i].Weight < edges[j].Weight) ||
			(edges[i].Weight == edges[j].Weight && edges[i].Src < edges[j].Src) ||
			(edges[i].Weight == edges[j].Weight && edges[i].Src == edges[j].Src && edges[i].Dest < edges[j].Dest)
	})
}

func WriteGraph(fileName string, edges []Edge) error {
	SortEdges(edges)

	file, err := os.Create(fileName)
	if err != nil {
		return err
	}
	defer file.Close()

	totalWeight := 0
	writer := bufio.NewWriter(file)
	for _, edge := range edges {
		totalWeight += edge.Weight
		_, err := fmt.Fprintf(writer, "%d %d %d\n", edge.Src, edge.Dest, edge.Weight)
		if err != nil {
			return err
		}
	}
	_, err = fmt.Fprintf(writer, "weight: %d\n", totalWeight)
	if err != nil {
		return err
	}

	return writer.Flush()
}
