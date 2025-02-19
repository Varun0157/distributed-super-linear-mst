package utils

import (
	"fmt"
	mst "mst/superlinear/utils/mst"
)

type Kruskals struct {
	NumVertices int
	Edges       []Edge
}

func NewKruskals(edges []Edge, numVertices int) (*Kruskals, error) {
	return &Kruskals{NumVertices: numVertices, Edges: edges}, nil
}

func (K *Kruskals) ConstructMST() ([]Edge, error) {
	edges := K.Edges
	SortEdges(edges)

	subsets := make(map[int]*mst.Subset)
	for parent := 1; parent <= K.NumVertices; parent++ {
		subset, err := mst.NewSubset(parent)
		if err != nil {
			return nil, fmt.Errorf("unable to create subset: %v", err)
		}
		subsets[parent] = subset
	}

	res := make([]Edge, 0)
	for _, edge := range edges {
		x := mst.Find(subsets, edge.Src)
		y := mst.Find(subsets, edge.Dest)

		if x == y {
			continue
		}

		res = append(res, edge)
		if len(res) == (K.NumVertices - 1) {
			break
		}
		mst.Union(subsets, x, y)
	}

	return res, nil
}
