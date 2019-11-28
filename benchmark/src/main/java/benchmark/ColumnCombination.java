package benchmark;

import com.google.common.collect.Sets;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.Set;

public class ColumnCombination {
  public String x;
  public String y;

  public ColumnCombination(String x, String y) {
    this.x = x;
    this.y = y;
  }

  public static Set<ColumnCombination> createColumnCombinations(
      Set<Set<String>> allColumns, Boolean intraDatasetCombinations, int maxSamples) {

    Set<ColumnCombination> result = new HashSet<>();

    if (intraDatasetCombinations) {
      for (Set<String> c : allColumns) {
        Set<Set<String>> intraCombinations = Sets.combinations(c, 2);
        for (Set<String> columnPair : intraCombinations) {
          result.add(createColumnCombination(columnPair));
        }
      }
    } else {
      Set<String> columnsSet = new HashSet<>();
      for (Set<String> c : allColumns) {
        columnsSet.addAll(c);
      }
      Set<Set<String>> interCombinations = Sets.combinations(columnsSet, 2);
      for (Set<String> columnPair : interCombinations) {
        result.add(createColumnCombination(columnPair));
      }
    }

    return result.size() <= maxSamples ? result : sample(result, maxSamples);
  }

  private static ColumnCombination createColumnCombination(Set<String> columnPair) {
    Iterator<String> it = columnPair.iterator();
    String x = it.next();
    String y = it.next();
    return new ColumnCombination(x, y);
  }

  /**
   * Perform sampling using reservoir sampling algorithm. If number os combinations is smaller than
   * the total number of desired samples, all combinations are kept. Otherwise, a random sample of
   * size numSamples is returned.
   *
   * @param combinations
   * @param numSamples
   * @return
   */
  public static Set<ColumnCombination> sample(Set<ColumnCombination> combinations, int numSamples) {
    List<ColumnCombination> reservoir = new ArrayList<>(numSamples);
    Random random = new Random(0);
    int numItemsSeen = 0;
    for (ColumnCombination item : combinations) {
      if (reservoir.size() < numSamples) {
        // reservoir not yet full, just append
        reservoir.add(item);
      } else {
        // find a sample to replace
        int randomIndex = random.nextInt(numItemsSeen + 1);
        if (randomIndex < numSamples) {
          reservoir.set(randomIndex, item);
        }
      }
      numItemsSeen++;
    }
    return new HashSet<>(reservoir);
  }
}
