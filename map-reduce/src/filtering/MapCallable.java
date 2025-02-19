package filtering;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;

public class MapCallable implements Callable<Map<String, Integer>> {
  private final String filePath;
  private final Map<String, Integer> logMap;

  public MapCallable(String filePath) {
    this.filePath = filePath;
    this.logMap = new HashMap<>();
  }

  @Override
  public Map<String, Integer> call() throws Exception {
    BufferedReader reader = new BufferedReader(new FileReader(this.filePath));

    String line;
    while ((line = reader.readLine()) != null) {
      String key = line.trim();
      int valueToPut = this.logMap.getOrDefault(key, 0) + 1;
      this.logMap.put(key, valueToPut);
    }

    reader.close();

    return this.logMap;
  }
}
