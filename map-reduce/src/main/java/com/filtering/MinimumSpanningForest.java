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
import java.util.*;

public class MinimumSpanningForest {
  private static final String NUM_REDUCERS_NAME = "numReducers";

  public static class MSTMapper extends Mapper<Object, Text, IntWritable, Text> {
    private List<Edge> edges = new ArrayList<>();
    private Random random = new Random();
    private int numReducers;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
      super.setup(context);
      numReducers = context.getConfiguration().getInt(NUM_REDUCERS_NAME, -1);

      if (numReducers <= 0) {
        throw new IOException("number of reducers not set to a positive value");
      }
    }

    @Override
    public void run(Context context) throws IOException, InterruptedException {
      setup(context);
      // Read all edges in the input split
      while (context.nextKeyValue()) {
        String line = context.getCurrentValue().toString().trim();
        String[] parts = line.split("\\s+");
        if (parts.length != 3) {
          // Skip invalid lines
          continue;
        }
        String src = parts[0];
        String dest = parts[1];
        int weight = Integer.parseInt(parts[2]);
        edges.add(new Edge(src, dest, weight));
      }

      // Sort edges by weight
      Collections.sort(edges);

      // Apply Kruskal's algorithm to find MST
      UnionFind uf = new UnionFind();
      List<Edge> mstEdges = new ArrayList<>();

      for (Edge edge : edges) {
        String rootSrc = uf.find(edge.src);
        String rootDest = uf.find(edge.dest);
        if (!rootSrc.equals(rootDest)) {
          mstEdges.add(edge);
          uf.union(rootSrc, rootDest);
        }
      }

      for (Edge edge : mstEdges) {
        int reducerNum = random.nextInt(numReducers);
        context.write(new IntWritable(reducerNum), new Text(edge.toString()));
      }
      cleanup(context);
    }

    static class Edge implements Comparable<Edge> {
      String src;
      String dest;
      int weight;

      Edge(String src, String dest, int weight) {
        this.src = src;
        this.dest = dest;
        this.weight = weight;
      }

      @Override
      public int compareTo(Edge o) {
        return Integer.compare(this.weight, o.weight);
      }

      @Override
      public String toString() {
        return src + " " + dest + " " + weight;
      }
    }

    static class UnionFind {
      private Map<String, String> parent = new HashMap<>();

      public String find(String node) {
        if (!parent.containsKey(node)) {
          parent.put(node, node);
          return node;
        }
        if (!parent.get(node).equals(node)) {
          parent.put(node, find(parent.get(node)));
        }
        return parent.get(node);
      }

      public void union(String a, String b) {
        String rootA = find(a);
        String rootB = find(b);
        if (!rootA.equals(rootB)) {
          parent.put(rootA, rootB);
        }
      }
    }
  }

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

  public static boolean runMSF(String inputPath, String outputPath, int numReducers) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "Minimum Spanning Forest");

    job.setJarByClass(MinimumSpanningForest.class);
    job.setMapperClass(MSTMapper.class);
    job.setReducerClass(EdgeReducer.class);
    job.getConfiguration().setInt(NUM_REDUCERS_NAME, numReducers);
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
