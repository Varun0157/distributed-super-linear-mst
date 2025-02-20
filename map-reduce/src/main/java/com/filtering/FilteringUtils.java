import java.io.*;
import java.util.HashSet;
import java.util.Set;

public class FilteringUtils {

  /**
   * Splits the input graph file into multiple files.
   * Each file will contain at most n^(1 + epsilon) edges,
   * where n is the number of unique vertices in the graph.
   *
   * @param inputFilePath Path to the input file containing graph edges.
   * @param outputDirPath Directory where the output files will be stored.
   * @param epsilon       The epsilon parameter to control the maximum edges per
   *                      file.
   * @throws IOException If an I/O error occurs.
   */
  public static void splitGraph(String inputFilePath, String outputDirPath, double epsilon) throws IOException {
    // First pass: count unique vertices and total edges.
    Set<Integer> vertices = new HashSet<>();
    int totalEdges = 0;

    try (BufferedReader reader = new BufferedReader(new FileReader(inputFilePath))) {
      String line;
      while ((line = reader.readLine()) != null) {
        GraphUtils.Edge edge = GraphUtils.Edge.read(line);
        vertices.add(edge.src);
        vertices.add(edge.dest);
        totalEdges++;
      }
    }

    int numVertices = vertices.size();
    int maxEdgesPerFile = (int) Math.ceil(Math.pow(numVertices, 1 + epsilon));

    System.out.println("total unique vertices: " + numVertices);
    System.out.println("total edges: " + totalEdges);
    System.out.println("max edges per file: " + maxEdgesPerFile);

    // Ensure output directory exists
    File outputDir = new File(outputDirPath);
    if (!outputDir.exists() && !outputDir.mkdirs()) {
      throw new IOException("failed to create output directory: " + outputDirPath);
    }

    // Second pass: split the input file into multiple files.
    int fileIndex = 0;
    int edgeCountInCurrentFile = 0;
    File currentOutputFile = new File(outputDir, "file" + fileIndex);
    BufferedWriter writer = new BufferedWriter(new FileWriter(currentOutputFile));

    try (BufferedReader reader = new BufferedReader(new FileReader(inputFilePath))) {
      String line;
      while ((line = reader.readLine()) != null) {
        if (edgeCountInCurrentFile >= maxEdgesPerFile) {
          writer.close();
          fileIndex++;
          edgeCountInCurrentFile = 0;
          currentOutputFile = new File(outputDir, "file" + fileIndex);
          writer = new BufferedWriter(new FileWriter(currentOutputFile));
        }
        writer.write(line);
        writer.newLine();
        edgeCountInCurrentFile++;
      }
    }
    writer.close();
    System.out.println("Graph successfully split into " + (fileIndex + 1) + " files.");
  }
}
