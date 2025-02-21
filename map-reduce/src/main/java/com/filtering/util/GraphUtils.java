package com.filtering.util;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class GraphUtils {
  public static class Edge implements Comparable<Edge> {
    public int src;
    public int dest;
    public int weight;

    public Edge(int src, int dest, int weight) {
      this.src = src;
      this.dest = dest;
      this.weight = weight;
    }

    @Override
    public int compareTo(Edge o) {
      return Integer.compare(this.weight, o.weight);
    }

    @Override
    public String toString() {
      return src + " " + dest + " " + weight;
    }

    public static Edge read(String line) throws IOException {
      String[] parts = line.split("\\s+");
      if (parts.length != 3) {
        throw new IllegalArgumentException("invalid input line: " + line);
      }

      final int src = Integer.parseInt(parts[0]);
      final int dest = Integer.parseInt(parts[1]);
      final int weight = Integer.parseInt(parts[2]);
      return new Edge(src, dest, weight);
    }
  }

  public static class DisjointSetUnion {
    private Map<Integer, Integer> parent = new HashMap<>();

    public int find(int node) {
      if (!parent.containsKey(node))
        parent.put(node, node);

      if (parent.get(node) != node)
        parent.put(node, find(parent.get(node)));
      return parent.get(node);
    }

    public void union(int a, int b) {
      int rootA = find(a);
      int rootB = find(b);

      if (rootA == rootB)
        return;
      parent.put(rootA, rootB);
    }
  }
}
