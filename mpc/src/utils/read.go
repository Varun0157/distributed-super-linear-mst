package utils

import (
	"bufio"
	"fmt"
	"os"
	"strconv"
	"strings"
)

func readMetadata(scanner *bufio.Scanner) (int, int, error) {
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

func readEdges(scanner *bufio.Scanner) ([]Edge, error) {
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
	}

	return edges, nil
}

func ReadGraph(fileName string) ([]Edge, int, error) {
	file, err := os.Open(fileName)
	if err != nil {
		return nil, 0, err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)

	numVertices, _, err := readMetadata(scanner)
	if err != nil {
		return nil, 0, err
	}
	edges, err := readEdges(scanner)
	if err != nil {
		return nil, 0, err
	}

	if err = scanner.Err(); err != nil {
		return nil, 0, err
	}

	return edges, numVertices, nil
}
