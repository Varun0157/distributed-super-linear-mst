package com.filtering.mst;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.List;
import java.util.ArrayList;
import java.util.Collections;

import com.filtering.util.GraphUtils;
import com.filtering.util.GeneralUtils;

public class MinimumSpanningForest {
  private static final String NUM_REDUCERS_KEY = "numReducers";

  public static class MSTMapper extends Mapper<Object, Text, IntWritable, Text> {
    private List<GraphUtils.Edge> edges = new ArrayList<>();
    private int numReducers;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
      super.setup(context);
      numReducers = context.getConfiguration().getInt(NUM_REDUCERS_KEY, -1);

      if (numReducers <= 0) {
        throw new IOException("number of reducers not set to a positive value");
      }
    }

    private List<GraphUtils.Edge> getLocalMSF(List<GraphUtils.Edge> edges) {
      Collections.sort(edges);
      GraphUtils.UnionFind uf = new GraphUtils.UnionFind();

      List<GraphUtils.Edge> msfEdges = new ArrayList<>();
      for (GraphUtils.Edge edge : edges) {
        int rootSrc = uf.find(edge.src);
        int rootDest = uf.find(edge.dest);

        if (rootSrc == rootDest) {
          continue;
        }
        msfEdges.add(edge);
        uf.union(rootSrc, rootDest);
      }

      return msfEdges;
    }

    @Override
    public void run(Context context) throws IOException, InterruptedException {
      setup(context);
      while (context.nextKeyValue()) {
        String line = context.getCurrentValue().toString().trim();
        GraphUtils.Edge edge = GraphUtils.Edge.read(line);
        edges.add(edge);
      }

      List<GraphUtils.Edge> msfEdges = getLocalMSF(edges);
      Collections.shuffle(msfEdges); // shuffle the edges before split to introduce randomness
      final List<List<GraphUtils.Edge>> splitEdges = GeneralUtils.splitList(msfEdges, numReducers);
      for (int reducer = 0; reducer < numReducers; reducer++) {
        for (GraphUtils.Edge edge : splitEdges.get(reducer)) {
          context.write(new IntWritable(reducer), new Text(edge.toString()));
        }
      }
      cleanup(context);
    }
  }

  // simply writes the mapped values
  public static class EdgeReducer extends Reducer<IntWritable, Text, Text, NullWritable> {
    private NullWritable nullValue = NullWritable.get();

    @Override
    public void reduce(IntWritable key, Iterable<Text> values, Context context)
        throws IOException, InterruptedException {
      for (Text value : values) {
        context.write(value, nullValue);
      }
    }
  }

  public static boolean calculateMSF(String inputPath, String outputPath, int numReducers) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "Minimum Spanning Forest");

    job.setJarByClass(MinimumSpanningForest.class);
    job.setMapperClass(MSTMapper.class);
    job.setReducerClass(EdgeReducer.class);
    job.getConfiguration().setInt(NUM_REDUCERS_KEY, numReducers);
    job.setNumReduceTasks(numReducers);

    job.setMapOutputKeyClass(IntWritable.class);
    job.setMapOutputValueClass(Text.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(NullWritable.class);

    FileInputFormat.addInputPath(job, new Path(inputPath));
    FileOutputFormat.setOutputPath(job, new Path(outputPath));

    return job.waitForCompletion(true);
  }
}
