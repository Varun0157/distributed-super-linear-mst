package utils

import (
	"bufio"
	"fmt"
	"strconv"
	"strings"
	"sync"
)

func ReadMetadata(scanner *bufio.Scanner) (int, int, error) {
	if !scanner.Scan() {
		return 0, 0, fmt.Errorf("unable to read metadata line")
	}

	meta := strings.Fields(scanner.Text())
	if len(meta) != 2 {
		return 0, 0, fmt.Errorf("invalid metadata line: %s", scanner.Text())
	}

	numVertices, err := strconv.Atoi(meta[0])
	if err != nil {
		return 0, 0, fmt.Errorf("invalid number of vertices: %s", meta[0])
	}
	numEdges, err := strconv.Atoi(meta[1])
	if err != nil {
		return 0, 0, fmt.Errorf("invalid number of edges: %s", meta[1])
	}

	return numVertices, numEdges, nil
}

func CreateEdgesGenerator(scanner *bufio.Scanner, numEdges, numNodes int) func() ([]Edge, error) {
	createPartition := CreatePartitionGenerator(numEdges, numNodes)

	fileMutex := sync.Mutex{}
	generateEdges := func() ([]Edge, error) {
		fileMutex.Lock()
		defer fileMutex.Unlock()

		start, end, err := createPartition()
		if err != nil {
			return nil, err
		}
		edgeBatchSize := end - start

		var edges []Edge
		for scanner.Scan() {
			parts := strings.Fields(scanner.Text())
			if len(parts) != 3 {
				return nil, fmt.Errorf("invalid line: %s", scanner.Text())
			}

			src, err1 := strconv.Atoi(parts[0])
			dest, err2 := strconv.Atoi(parts[1])
			weight, err3 := strconv.Atoi(parts[2])

			if err1 != nil || err2 != nil || err3 != nil {
				return nil, fmt.Errorf("invalid line: %s", scanner.Text())
			}

			edges = append(edges, NewEdge(src, dest, weight))
			if len(edges) == edgeBatchSize {
				break
			}
		}

		return edges, nil
	}

	return generateEdges
}
