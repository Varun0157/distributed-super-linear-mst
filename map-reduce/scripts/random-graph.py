from typing import List, Tuple

import random
import argparse


def generate_graph(n, m):
    edges = set()

    while len(edges) < m:
        u = random.randint(1, n)
        v = random.randint(1, n)

        if u != v and (u, v) not in edges:
            edges.add((u, v))

    graph: List[Tuple[int, int, int]] = []
    for u, v in edges:
        weight = random.randint(1, 1000)
        graph.append((u, v, weight))

    return graph


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate a weighted graph.")
    parser.add_argument("n", type=int, help="Number of nodes")
    parser.add_argument("m", type=int, help="Number of edges")
    parser.add_argument("output", type=str, help="Output file name")
    parser.add_argument("num_files", type=int, help="number of files")
    print("warning: assumes m % num_files is 0 for now")

    args = parser.parse_args()
    graph = generate_graph(args.n, args.m)
    edges_per_file = len(graph) // args.num_files
    for num in range(args.num_files):
        file_name = args.output + str(num) + ".txt"

        graph_subset = graph[num * edges_per_file : (num + 1) * edges_per_file]
        with open(file_name, "w") as f:
            for u, v, weight in graph_subset:
                f.write(f"{u} {v} {weight}\n")
