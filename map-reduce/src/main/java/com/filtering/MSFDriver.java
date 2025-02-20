import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;

public class MSFDriver {
  public static void main(String[] args) throws Exception {
    // Base directory where data, and results reside
    String basePath = "file:///home/varun.edachali/map-reduce/";
    // Initial input is in the "data" directory.
    String inputPath = basePath + "data";
    int round = 0;

    // Create Hadoop configuration and filesystem objects.
    Configuration conf = new Configuration();
    conf.set("fs.defaultFS", "file:///");
    FileSystem fs = FileSystem.get(conf);

    while (true) {
      // List the files in the current input directory.
      FileStatus[] inputFiles = fs.listStatus(new Path(inputPath));
      int numFiles = inputFiles.length;
      System.out.println("Round " + round + ": " + numFiles + " files found in " + inputPath);

      // Stop if there is only one file remaining.
      if (numFiles <= 1) {
        System.out.println("Only one file remains. Terminating iterations.");
        break;
      }

      double reductionFactor = Math.pow(10, 1.1);
      int numReducers = (int) Math.ceil((double) numFiles / reductionFactor);
      String outputPath = basePath + "res_" + round;
      System.out.println("Running job with " + numReducers + " reducers, output will be stored in " + outputPath);

      // Delete output directory if it already exists.
      Path outPath = new Path(outputPath);
      if (fs.exists(outPath)) {
        fs.delete(outPath, true);
      }

      // Run the MinimumSpanningForest job.
      // Note: The runMSF method should be updated to use the passed input and output
      // paths.
      boolean success = MinimumSpanningForest.runMSF(inputPath, outputPath, numReducers);
      if (!success) {
        System.err.println("Job failed in round " + round);
        System.exit(1);
      }

      // Set next input to be the output from this round.
      inputPath = outputPath;
      round++;
    }
  }
}
