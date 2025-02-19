package mst

type Subset struct {
	Parent, Children int
}

func NewSubset(Parent int) (*Subset, error) {
	return &Subset{Parent: Parent, Children: 0}, nil
}

func Find(subsets map[int]*Subset, i int) int {
	if subsets[i].Parent != i {
		subsets[i].Parent = (Find(subsets, subsets[i].Parent))
	}
	return subsets[i].Parent
}

func Union(subsets map[int]*Subset, x, y int) {
	xroot := Find(subsets, x)
	yroot := Find(subsets, y)

	if xroot == yroot {
		return
	}

	if subsets[xroot].Children < subsets[yroot].Children {
		subsets[xroot].Parent = yroot
		subsets[yroot].Children += subsets[xroot].Children
	} else {
		subsets[yroot].Parent = xroot
		subsets[xroot].Children += subsets[yroot].Children
	}
}
