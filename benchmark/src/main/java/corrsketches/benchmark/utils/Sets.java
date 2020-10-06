package corrsketches.benchmark.utils;

import java.util.HashSet;
import java.util.Set;

public class Sets {

  /**
   * Computes the size of the intersection of the given sets with good algorithmic complexity and
   * without performing any set copies.
   */
  public static <T> int intersectionSize(HashSet<T> x, HashSet<T> y) {
    Set<T> largerSet;
    Set<T> smallerSet;
    if (x.size() < y.size()) {
      largerSet = x;
      smallerSet = y;
    } else {
      largerSet = y;
      smallerSet = x;
    }
    int size = 0;
    for (T e : smallerSet) {
      if (largerSet.contains(e)) {
        size++;
      }
    }
    return size;
  }

  /**
   * Computes the size of the union of the given sets with good algorithmic complexity and without
   * performing any set copies.
   */
  public static <T> int unionSize(HashSet<T> x, HashSet<T> y) {
    Set<T> largerSet;
    Set<T> smallerSet;
    if (x.size() < y.size()) {
      largerSet = x;
      smallerSet = y;
    } else {
      largerSet = y;
      smallerSet = x;
    }
    int size = largerSet.size();
    for (T e : smallerSet) {
      if (!largerSet.contains(e)) {
        size++;
      }
    }
    return size;
  }
}
