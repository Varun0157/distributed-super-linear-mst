import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.*;

public class MinimumSpanningForest {
  public static class Edge {
    int src;
    int dest;
    int weight;

    public Edge(int s, int d, int w) {
      this.src = s;
      this.dest = d;
      this.weight = w;
    }

    @Override
    public String toString() {
      return src + " " + dest + " " + weight;
    }
  }

  // The mapper collects edges, computes a local MST (forest) via Kruskal's,
  // and emits each selected edge with a random key.
  public static class MSTMapper extends Mapper<Object, Text, IntWritable, Text> {
    private List<Edge> edgeList = new ArrayList<>();
    private Random random = new Random();
    // hardcode number of reducers (i.e. target nodes) to 2 as requested
    private int numReducers = 2;

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      // Each input line is assumed to be in the form "src dest weight"
      String line = value.toString().trim();
      if (line.isEmpty())
        return;
      String[] parts = line.split("\\s+");
      if (parts.length != 3)
        return;
      try {
        int src = Integer.parseInt(parts[0]);
        int dest = Integer.parseInt(parts[1]);
        int weight = Integer.parseInt(parts[2]);
        edgeList.add(new Edge(src, dest, weight));
      } catch (NumberFormatException e) {
        // skip malformed lines
      }
    }

    // Standard union-find "find" with path compression.
    private int find(Map<Integer, Integer> parent, int i) {
      if (parent.get(i) != i) {
        parent.put(i, find(parent, parent.get(i)));
      }
      return parent.get(i);
    }

    // Standard union operation.
    private void union(Map<Integer, Integer> parent, int x, int y) {
      int xroot = find(parent, x);
      int yroot = find(parent, y);
      parent.put(xroot, yroot);
    }

    // In cleanup we have seen all lines for this mapper.
    // We now perform Kruskal’s algorithm to compute a local MST forest.
    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
      // Gather all vertices from the edges
      Set<Integer> vertices = new HashSet<>();
      for (Edge edge : edgeList) {
        vertices.add(edge.src);
        vertices.add(edge.dest);
      }
      // Initialize union-find parent pointers.
      Map<Integer, Integer> parent = new HashMap<>();
      for (Integer v : vertices) {
        parent.put(v, v);
      }
      // Sort edges by weight (ascending)
      Collections.sort(edgeList, new Comparator<Edge>() {
        public int compare(Edge e1, Edge e2) {
          return Integer.compare(e1.weight, e2.weight);
        }
      });
      // Run Kruskal’s algorithm.
      List<Edge> mstEdges = new ArrayList<>();
      for (Edge edge : edgeList) {
        int x = find(parent, edge.src);
        int y = find(parent, edge.dest);
        if (x != y) {
          mstEdges.add(edge);
          union(parent, x, y);
        }
      }
      // Emit each MST edge with a random key (to randomly distribute among reducers).
      for (Edge edge : mstEdges) {
        int randomKey = random.nextInt(numReducers);
        context.write(new IntWritable(randomKey), new Text(edge.toString()));
      }
    }
  }

  // The reducer simply outputs the received edges.
  public static class RandomDistributorReducer extends Reducer<IntWritable, Text, Text, Text> {
    @Override
    public void reduce(IntWritable key, Iterable<Text> values, Context context)
        throws IOException, InterruptedException {
      for (Text edge : values) {
        // Emit edge as key and an empty string as value
        context.write(edge, new Text(""));
      }
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "minimum spanning forest");
    job.setJarByClass(MinimumSpanningForest.class);
    job.setMapperClass(MSTMapper.class);
    job.setReducerClass(RandomDistributorReducer.class);
    // Set the number of reducers to 2 (as required)
    job.setNumReduceTasks(2);

    // Mapper output types
    job.setMapOutputKeyClass(IntWritable.class);
    job.setMapOutputValueClass(Text.class);
    // Reducer output types
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);

    // Use local file system paths (update these as needed)
    FileInputFormat.addInputPath(job, new Path("file:///home/varun.edachali/map-reduce/data"));
    FileOutputFormat.setOutputPath(job, new Path("file:///home/varun.edachali/map-reduce/res"));

    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
