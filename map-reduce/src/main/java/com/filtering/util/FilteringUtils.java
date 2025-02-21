package com.filtering.util;

import java.util.List;
import java.util.Set;
import java.util.HashSet;
import java.util.ArrayList;
import java.util.Collections;
import java.io.File;
import java.io.IOException;
import java.io.FileReader;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileWriter;

public class FilteringUtils {
  public static MetaData splitGraph(String inputFilePath, String outputDirPath, double epsilon) throws IOException {
    // First pass: count unique vertices and total edges.
    Set<Integer> vertices = new HashSet<>();
    int numEdges = 0;

    try (BufferedReader reader = new BufferedReader(new FileReader(inputFilePath))) {
      String line;
      while ((line = reader.readLine()) != null) {
        GraphUtils.Edge edge = GraphUtils.Edge.read(line);
        vertices.add(edge.src);
        vertices.add(edge.dest);
        numEdges++;
      }
    }

    final int totalVertices = vertices.size();
    final int totalEdges = numEdges;
    MetaData md = new MetaData(totalVertices, totalEdges, epsilon);

    System.out.println("total unique vertices: " + md.getTotalVertices());
    System.out.println("total edges: " + md.getTotalEdges());
    System.out.println("max edges per file: " + md.S());

    // Ensure output directory exists
    File outputDir = new File(outputDirPath);
    if (!outputDir.exists() && !outputDir.mkdirs()) {
      throw new IOException("failed to create output directory: " + outputDirPath);
    }

    // Second pass: split the input file into multiple files.
    final int numFiles = md.getNumComputationalNodes(0);
    List<String> lines = new ArrayList<>();
    try (BufferedReader reader = new BufferedReader(new FileReader(inputFilePath))) {
      String line;
      while ((line = reader.readLine()) != null) {
        lines.add(line);
      }
    }
    Collections.shuffle(lines);
    final List<List<String>> splitLines = GeneralUtils.splitList(lines, numFiles);

    for (int fileIndex = 0; fileIndex < numFiles; fileIndex++) {
      File outputFile = new File(outputDirPath, "part-" + fileIndex);
      try (BufferedWriter writer = new BufferedWriter(new FileWriter(outputFile))) {
        for (String line : splitLines.get(fileIndex)) {
          writer.write(line);
          writer.newLine();
        }
        writer.close();
      } catch (IOException e) {
        throw new IOException("failed to write to file: " + outputFile.getAbsolutePath(), e);
      }
    }

    System.out.println("graph successfully split into " + numFiles + " files.");

    return md;
  }
}
