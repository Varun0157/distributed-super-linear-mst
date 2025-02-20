import java.io.File;
import java.io.IOException;
import java.nio.file.DirectoryStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.eclipse.jetty.util.IO;

public class MSFDriver {
  private static void deleteMetaDataFiles(String dirPath, String prefix) throws IOException {
    File dir = new File(dirPath);
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
  }

  private static void calculateMST(String inputDir, String outputPrefix, float epsilon) throws Exception {
    final String basePath = "file:///home/varun.edachali/map-reduce/";
    final String inputPath = basePath + inputDir;

    Configuration conf = new Configuration();
    conf.set("fs.defaultFS", "file:///"); // use local file system instead of hdfs
    FileSystem fs = FileSystem.get(conf);

    int round = 0;
    while (true) {
      FileStatus[] inputFiles = fs.listStatus(new Path(inputPath));
      int numFiles = inputFiles.length;
      System.out.println("round " + round + ": " + numFiles + " files found in " + inputPath);

      if (numFiles <= 1) {
        System.out.println("only one file remains. Terminating iterations.");
        break;
      }

      final double reductionFactor = Math.pow(10, 1 + epsilon);
      final int numReducers = (int) Math.ceil((double) numFiles / reductionFactor);
      final String outputPath = basePath + outputPrefix + round;
      System.out.println("running job with " + numReducers + " reducers, output will be stored in " + outputPath);

      // the mapreduce invoked requires the output path to not exist
      Path outPath = new Path(outputPath);
      if (fs.exists(outPath)) {
        fs.delete(outPath, true);
      }

      boolean success = MinimumSpanningForest.runMSF(inputPath, outputPath, numReducers);
      if (!success) {
        System.err.println("job failed in round " + round);
        System.exit(1);
      }
      // goal: get rid of the _SUCCESSS files
      deleteMetaDataFiles(outputPath, "_");

      inputPath = outputPath;
      round++;
    }
  }

  public static void main(String[] args) throws Exception {
    if (args.length != 2) {
      System.out.println("Usage: MSFDriver <input_dir> <output_prefix> <epsilon>");
      System.exit(-1);
    }
    final String inputDir = args[0];
    final String outputPrefix = args[1];
    final float epsilon = Float.parseFloat(args[2]);

    calculateMST(inputDir, outputPrefix, epsilon);
  }
}
