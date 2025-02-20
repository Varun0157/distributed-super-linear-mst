import java.io.*;
import java.util.HashSet;
import java.util.Set;

public class FilteringUtils {
  public static MetaData splitGraph(String inputFilePath, String outputDirPath, double epsilon) throws IOException {
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

    final int numVertices = vertices.size();
    MetaData md = new MetaData(numVertices, totalEdges, epsilon);

    System.out.println("total unique vertices: " + md.getTotalVertices());
    System.out.println("total edges: " + md.getTotalEdges());
    System.out.println("max edges per file: " + md.S());

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
        if (edgeCountInCurrentFile >= md.S()) {
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

    final int numFiles = fileIndex + 1;
    if (numFiles != md.getNumComputationalNodes(0)) {
      throw new IOException("number of files (" + numFiles + ") does not match number of computational nodes ("
          + md.getNumComputationalNodes(0) + ")");
    }
    System.out.println("graph successfully split into " + numFiles + " files.");

    return md;
  }
}
