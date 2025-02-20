import java.util.HashMap;
import java.util.Map;

public class GraphUtils {
  public static class Edge implements Comparable<Edge> {
    public String src;
    public String dest;
    public int weight;

    public Edge(String src, String dest, int weight) {
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

  public static class UnionFind {
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
