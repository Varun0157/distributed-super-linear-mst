package com.filtering.mst;

import java.io.File;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;

import com.filtering.util.MetaData;
import com.filtering.util.FilteringUtils;

public class MinimumSpanningTree {
  private static void calculateMST(String inputDir, String outputPrefix, MetaData md, String localBasePath)
      throws Exception {
    final String basePath = "file://" + localBasePath;
    String inputPath = new File(basePath, inputDir).toString();

    Configuration conf = new Configuration();
    conf.set("fs.defaultFS", "file:///"); // use local file system instead of hdfs
    FileSystem fs = FileSystem.get(conf);

    int round = 1;
    while (true) {
      FileStatus[] inputFiles = fs.listStatus(new Path(inputPath));
      final int numInputFiles = inputFiles.length;
      System.out.println("round " + round + ": " + numInputFiles + " files found in " + inputPath);

      final int numReducers = md.getNumComputationalNodes(round);
      final String outputPath = new File(basePath, outputPrefix + round).toString();

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

      if (numInputFiles <= 1) {
        System.out.println();
        System.out.println("-- only one input file was present -> terminating iterations after " + round + " rounds.");
        break;
      }

      inputPath = outputPath;
      round++;
    }
  }

  public static void main(String[] args) throws Exception {
    if (args.length != 5) {
      System.out.println("usage args: <graph_path> <localBasePath> <input_dir> <output_prefix> <epsilon>");
      System.exit(-1);
    }
    final String graphPath = args[0];
    final String localBasePath = args[1];
    final String inputDir = args[2];
    final String outputPrefix = args[3];
    final float epsilon = Float.parseFloat(args[4]);

    MetaData md = FilteringUtils.splitGraph(graphPath, inputDir, epsilon);
    md.printRoundDetails();
    calculateMST(inputDir, outputPrefix, md, localBasePath);
  }
}
