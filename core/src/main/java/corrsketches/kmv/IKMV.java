package corrsketches.kmv;

import corrsketches.util.Hashes;
import java.util.HashSet;
import java.util.List;
import java.util.TreeSet;

public interface IKMV<T> {

  /**
   * Updates this synopsis with the hashes of all the given key strings and their associated values
   */
  default void updateAll(List<String> keys, double[] values) {
    if (keys.size() != values.length) {
      throw new IllegalArgumentException("keys and values must have equal size.");
    }
    for (int i = 0; i < values.length; i++) {
      this.update(keys.get(i), values[i]);
    }
  }

  /**
   * Updates this synopsis with the hash value (Murmur3) of the given key string and its associated
   * value.
   */
  default void update(String key, double value) {
    if (key == null || key.isEmpty()) {
      return;
    }
    int keyHash = Hashes.murmur3_32(key);
    this.update(keyHash, value);
  }

  /** Updates this synopsis with the given hashed key */
  void update(int hash, double value);

  double distinctValues();

  /** Estimates the size of union of the given KMV synopsis */
  double unionSize(T other);

  /** Estimates intersection between the sets represented by this synopsis and the other. */
  double intersectionSize(T other);

  /** Estimates the Jaccard similarity between this and the other synopsis */
  double jaccard(T other);

  /**
   * Estimates the jaccard containment (JC) of the set represented by this synopsis with the other
   * synopsis.
   *
   * <p>JC(X, Y) = |X ∩ Y| / |X| = |this ∩ other| / |this|
   */
  default double containment(T other) {
    return this.intersectionSize(other) / this.distinctValues();
  }

  TreeSet<ValueHash> getKMinValues();

  default int intersectionSize(TreeSet<ValueHash> x, TreeSet<ValueHash> y) {
    HashSet<ValueHash> intersection = new HashSet<>(x);
    intersection.retainAll(y);
    return intersection.size();
  }

  default int unionSize(TreeSet<ValueHash> x, TreeSet<ValueHash> y) {
    HashSet<ValueHash> union = new HashSet<>(x);
    union.addAll(y);
    int k = union.size();
    if (k < 1) {
      throw new IllegalStateException(
          String.format(
              "Can not compute estimates on empty synopsis. x.size=[%d] y.size=[%d]",
              x.size(), y.size()));
    }
    return k;
  }
}
