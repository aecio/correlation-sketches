package corrsketches.kmv;

import corrsketches.aggregations.AggregateFunction;
import corrsketches.util.Hashes;
import it.unimi.dsi.fastutil.doubles.DoubleArrayList;
import it.unimi.dsi.fastutil.doubles.DoubleComparators;
import java.util.List;
import java.util.TreeSet;

/**
 * Implements the KMV synopsis from the paper "On Synopsis for distinct-value estimation under
 * multiset operations" by Beyer et. at, SIGMOD, 2017.
 */
public class KMV extends IKMV<KMV> {

  public static final int DEFAULT_K = 256;
  private final int maxK;

  // TODO: Replace all constructors by a builder class
  @Deprecated
  public KMV(int k) {
    this(k, AggregateFunction.FIRST);
  }

  public KMV(int k, AggregateFunction function) {
    super(k, function);
    if (k < 1) {
      throw new IllegalArgumentException("Minimum k size is 1, but larger is recommended.");
    }
    this.maxK = k;
  }

  public static KMV create(List<String> keys, double[] values) {
    return create(keys, values, DEFAULT_K);
  }

  public static KMV create(List<String> keys, double[] values, int k) {
    return create(keys, values, k, AggregateFunction.FIRST);
  }

  public static KMV create(List<String> keys, double[] values, AggregateFunction function) {
    return create(keys, values, DEFAULT_K, function);
  }

  public static KMV create(List<String> keys, double[] values, int k, AggregateFunction function) {
    if (keys.size() != values.length) {
      final String msg =
          String.format(
              "keys and values must have same size. keys.size=[%d] values.size=[%d]",
              keys.size(), values.length);
      throw new IllegalArgumentException(msg);
    }
    KMV kmv = new KMV(k, function);
    kmv.updateAll(keys, values);
    return kmv;
  }

  /** Creates a KMV synopsis of size k from an array of hashed keys. */
  public static KMV fromHashedKeys(int[] hashes, double[] values, int k) {
    KMV kmv = new KMV(k);
    for (int i = 0; i < hashes.length; i++) {
      kmv.update(hashes[i], values[i]);
    }
    return kmv;
  }

  /** Updates the KMV synopsis with the given hashed key */
  @Override
  public void update(int hash, double value) {
    final double hu = Hashes.grm(hash);
    if (kMinValues.size() < maxK) {
      ValueHash vh = createOrUpdateValueHash(hash, value, hu);
      kMinValues.add(vh);
      if (hu > kthValue) {
        kthValue = hu;
      }
    } else if (hu < kthValue) {
      ValueHash vh = createOrUpdateValueHash(hash, value, hu);
      kMinValues.add(vh);
      ValueHash toBeRemoved = kMinValues.last();
      kMinValues.remove(toBeRemoved);
      valueHashMap.remove(toBeRemoved.keyHash);
      kthValue = kMinValues.last().unitHash;
    }
  }

  /** Estimates the size of union of the given KMV synopsis */
  @Override
  public double unionSize(KMV other) {
    int k = computeK(this, other);
    double kthValue = kthValueOfUnion(this.kMinValues, other.kMinValues);
    return (k - 1) / kthValue;
  }

  /** Estimates the Jaccard similarity using the p = K_e / k estimator from Beyer et. al. (2007) */
  @Override
  public double jaccard(KMV other) {
    int k = computeK(this, other);
    int intersection = intersectionSize(this.kMinValues, other.kMinValues);
    return intersection / (double) k;
  }

  /** Estimates intersection between the sets represented by this synopsis and the other. */
  @Override
  public double intersectionSize(KMV other) {
    int k = computeK(this, other);
    // p is an unbiased estimate of the jaccard similarity
    double p = intersectionSize(this.kMinValues, other.kMinValues) / (double) k;
    // the k-th unit hash value of the union
    double kthValue = kthValueOfUnion(this.kMinValues, other.kMinValues);
    double u = (k - 1) / kthValue;
    // estimation of intersection size
    return p * u;
  }

  private static double kthValueOfUnion(TreeSet<ValueHash> x, TreeSet<ValueHash> y) {
    TreeSet<ValueHash> union = new TreeSet<>(ValueHash.COMPARATOR_ASC);
    union.addAll(x);
    union.addAll(y);

    int maxUnionSize = x.size() + y.size();
    DoubleArrayList values = new DoubleArrayList(maxUnionSize);
    for (ValueHash v : union) {
      values.add(v.unitHash);
    }
    values.sort(DoubleComparators.NATURAL_COMPARATOR);

    int k = Math.min(x.size(), y.size());
    return values.getDouble(k - 1);
  }

  private static int computeK(KMV x, KMV y) {
    int xSize = x.kMinValues.size();
    int ySize = y.kMinValues.size();
    int k = Math.min(xSize, ySize);
    if (k < 1) {
      throw new IllegalStateException(
          String.format(
              "Can not compute estimates on empty synopsis. x.size=[%d] y.size=[%d]",
              xSize, ySize));
    }
    return k;
  }

  @Override
  public String toString() {
    return "KMV{" + "maxK=" + maxK + ", kMinValues=" + kMinValues + ", kthValue=" + kthValue + '}';
  }
}
