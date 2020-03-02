package benchmark;

import com.google.common.collect.ImmutableList;
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
      Set<Set<String>> allColumns, Boolean intraDatasetCombinations, int maxColumnsSamples) {

    Set<ColumnCombination> result = new HashSet<>();

    if (intraDatasetCombinations) {
      for (Set<String> c : allColumns) {
        Set<Set<String>> intraCombinations = Sets.combinations(c, 2);
        for (Set<String> columnPair : intraCombinations) {
          result.add(createColumnCombination(columnPair));
        }
      }
    } else {
      // If there are more columns than maxColumnsSamples, create a sample of size maxColumnsSamples
      Sampler<String> sampler = new Sampler<>(maxColumnsSamples);
      for (Set<String> c : allColumns) {
        for (String s : c) {
          sampler.sample(s);
        }
      }
      Set<String> columnsSet = new HashSet<>(sampler.getSamples());
      Set<Set<String>> interCombinations = Sets.combinations(columnsSet, 2);
      for (Set<String> columnPair : interCombinations) {
        result.add(createColumnCombination(columnPair));
      }
    }

    return result;
  }

  private static ColumnCombination createColumnCombination(Set<String> columnPair) {
    Iterator<String> it = columnPair.iterator();
    String x = it.next();
    String y = it.next();
    return new ColumnCombination(x, y);
  }

  /**
   * Perform sampling using reservoir sampling algorithm. If number of items provided is smaller
   * than the total number of desired samples, all items are stored. Otherwise, a random sample of
   * size numSamples is stored.
   */
  static class Sampler<T> {

    private Random random;
    private int numSamples;
    private List<T> reservoir;
    int numItemsSeen = 0;

    public Sampler(int numSamples) {
      this.numSamples = numSamples;
      this.reservoir = new ArrayList<>(numSamples);
      this.random = new Random(0);
    }

    /**
     * Perform sampling using reservoir sampling algorithm. If number os combinations is smaller
     * than the total number of desired samples, all combinations are kept. Otherwise, a random
     * sample of size numSamples is returned.
     */
    public void sample(T item) {
      if (reservoir.size() < numSamples) {
        // when the reservoir not full, just append
        reservoir.add(item);
      } else {
        // when it is full, randomly select a sample to replace
        int randomIndex = random.nextInt(numItemsSeen + 1);
        if (randomIndex < numSamples) {
          reservoir.set(randomIndex, item);
        }
      }
      numItemsSeen++;
    }

    public List<T> getSamples() {
      return ImmutableList.copyOf(reservoir);
    }
  }
}
