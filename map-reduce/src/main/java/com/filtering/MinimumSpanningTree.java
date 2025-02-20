import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.DirectoryStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;

public class MinimumSpanningTree {
  private static void deleteMetaDataFiles(String dirPath, String prefix) throws IOException {
    try {
      File dir = new File(new URI(dirPath));
      if (!dir.exists()) {
        throw new IOException("directory does not exist: " + dirPath);
      }
      if (!dir.isDirectory()) {
        throw new IOException("not a directory: " + dirPath);
      }

      File[] files = dir.listFiles((directory, fileName) -> fileName.startsWith(prefix));
      if (files == null) {
        throw new IOException("unable to list files in directory: " + dirPath);
      }

      for (File file : files) {
        System.out.println("deleting " + file.getName());
        if (file.delete()) {
          continue;
        }
        throw new IOException("failed to delete " + file.getName());
      }
    } catch (URISyntaxException e) {
      throw new IOException("invalid uri syntax", e);
    }
  }

  private static void calculateMST(String inputDir, String outputPrefix, MetaData md) throws Exception {
    final String basePath = "file:///home/varun.edachali/map-reduce/";
    String inputPath = basePath + inputDir;

    Configuration conf = new Configuration();
    conf.set("fs.defaultFS", "file:///"); // use local file system instead of hdfs
    FileSystem fs = FileSystem.get(conf);

    int round = 1;
    while (true) {
      FileStatus[] inputFiles = fs.listStatus(new Path(inputPath));
      int numFiles = inputFiles.length;
      System.out.println("round " + round + ": " + numFiles + " files found in " + inputPath);

      final int numReducers = md.getNumComputationalNodes(round);
      final String outputPath = basePath + outputPrefix + round;
      System.out.println("running job with " + numReducers + " reducers, output will be stored in " + outputPath);

      // the mapreduce invoked requires the output path to not exist
      Path outPath = new Path(outputPath);
      if (fs.exists(outPath)) {
        fs.delete(outPath, true);
      }

      boolean success = MinimumSpanningForest.calculateMSF(inputPath, outputPath, numReducers);
      if (!success) {
        System.err.println("job failed in round " + round);
        System.exit(1);
      }
      // goal: get rid of the _SUCCESSS files
      deleteMetaDataFiles(outputPath, "_");

      if (numFiles <= 1) {
        System.out.println("only one input file was present. Terminating iterations.");
        break;
      }

      inputPath = outputPath;
      round++;
    }
  }

  public static void main(String[] args) throws Exception {
    if (args.length != 4) {
      System.out.println("usage args: <graph_path> <input_dir> <output_prefix> <epsilon>");
      System.exit(-1);
    }
    final String graphPath = args[0];
    final String inputDir = args[1];
    final String outputPrefix = args[2];
    final float epsilon = Float.parseFloat(args[3]);

    MetaData md = FilteringUtils.splitGraph(graphPath, inputDir, epsilon);
    md.printRoundDetails();
    calculateMST(inputDir, outputPrefix, md);
  }
}
