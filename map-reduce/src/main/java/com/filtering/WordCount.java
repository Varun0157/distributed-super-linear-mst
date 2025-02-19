package filtering;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.StringTokenizer;

public class WordCount {
  // Mapper class: processes each line of input
  public static class TokenizerMapper
      extends Mapper<LongWritable, Text, Text, IntWritable> {

    // Reusable output value (each word counts as 1)
    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    // The map method gets called once per input split record
    @Override
    public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {
      // Convert line to String and tokenize it
      StringTokenizer itr = new StringTokenizer(value.toString());
      while (itr.hasMoreTokens()) {
        // Set word and emit <word, 1>
        word.set(itr.nextToken());
        context.write(word, one);
      }
    }
  }

  // Reducer class: aggregates counts for each word
  public static class IntSumReducer
      extends Reducer<Text, IntWritable, Text, IntWritable> {

    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context)
        throws IOException, InterruptedException {
      int sum = 0;
      // Sum all the counts for the word (key)
      for (IntWritable val : values) {
        sum += val.get();
      }
      // Write the aggregated result to output
      context.write(key, new IntWritable(sum));
    }
  }

  // Driver: sets up and runs the job
  public static void main(String[] args) throws Exception {
    // Create configuration; you can add custom config parameters here
    Configuration conf = new Configuration();

    // Create a new Job and give it a name
    Job job = Job.getInstance(conf, "word count");
    job.setJarByClass(WordCount.class);

    // Specify Mapper and Reducer classes
    job.setMapperClass(TokenizerMapper.class);
    // Using the reducer as a combiner is optional but can improve performance
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);

    // Set the output key and value types for the job (both intermediate and final)
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);

    // Set the input and output paths on HDFS.
    // For example, you might hardcode these or pass them as command-line args.
    // Here we assume args[0] is the input path and args[1] is the output path.
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    // Submit the job to the cluster and wait for it to finish
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
