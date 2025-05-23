package com.filtering.util;

import java.util.HashMap;
import java.util.Map;

public class MetaData {
  private int totalVertices;
  private double epsilon;
  private int totalEdges;
  private Map<Integer, Integer> numEdgesStore = new HashMap<>();

  public MetaData(int numVertices, int numEdges, double epsilon) {
    this.totalVertices = numVertices;
    this.totalEdges = numEdges;
    this.epsilon = epsilon;
  }

  public int getTotalVertices() {
    return this.totalVertices;
  }

  public int getTotalEdges() {
    return this.totalEdges;
  }

  public double getEpsilon() {
    return this.epsilon;
  }

  public int S() {
    return (int) Math.ceil(Math.pow(this.totalVertices, 1 + this.epsilon));
  }

  public int getNumEdges(int round) {
    if (!numEdgesStore.containsKey(round)) {
      final int edges = round <= 0 ? this.totalEdges
          : (int) Math.ceil((double) getNumEdges(round - 1) / Math.pow(this.totalVertices, this.epsilon));
      numEdgesStore.put(round, edges);
    }
    return numEdgesStore.get(round);
  }

  public int getNumComputationalNodes(int round) {
    return (int) Math.ceil((double) getNumEdges(round) / S());
  }

  public void printRoundDetails() {
    int round = 0;
    while (true) {
      final int numEdges = getNumEdges(round);
      final int numComputationalNodes = getNumComputationalNodes(round);
      System.out
          .println("round " + round + ": " + numEdges + " edges, " + numComputationalNodes + " computational nodes");
      if (numComputationalNodes == 1) {
        break;
      }
      round += 1;
    }
  }
}
