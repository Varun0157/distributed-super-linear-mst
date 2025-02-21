import java.util.ArrayList;
import java.util.List;

public class GeneralUtils {
  public static <T> List<List<T>> splitList(List<T> list, int k) {
    if (list == null || k <= 0) {
      throw new IllegalArgumentException("list cannot be null and k must be greater than 0");
    }

    final int n = list.size();
    final int baseSize = n / k;
    final int remainder = n % k;

    List<List<T>> result = new ArrayList<>();
    int start = 0;
    for (int i = 0; i < k; i++) {
      final int end = start + baseSize + (i < remainder ? 1 : 0);
      if (start < n) {
        result.add(new ArrayList<>(list.subList(start, Math.min(end, n))));
      } else {
        result.add(new ArrayList<>());
      }
      start = end;
    }
    return result;
  }
}
