package sketches.kmv;

import it.unimi.dsi.fastutil.doubles.DoubleArrayList;
import it.unimi.dsi.fastutil.doubles.DoubleComparators;
import java.util.List;
import java.util.TreeSet;
import sketches.correlation.Hashes;

/**
 * Implements the KMV synopsis from the paper "On Synopsis fir distinct-value estimation under
 * multiset operations" by Beyer et. at, SIGMOD, 2017.
 */
public class KMV implements IKMV<KMV> {

  public static final int DEFAULT_K = 256;

  private final int maxK;
  private final TreeSet<ValueHash> kMinValues;
  private double kthValue = Double.MIN_VALUE;

  public KMV(int k) {
    if (k < 1) {
      throw new IllegalArgumentException("Minimum k size is 1, but larger is recommended.");
    }
    this.maxK = k;
    this.kMinValues = new TreeSet<>(ValueHash.COMPARATOR_ASC);
  }

  public static KMV create(List<String> keys, double[] values) {
    return create(keys, values, DEFAULT_K);
  }

  public static KMV create(List<String> keys, double[] values, int k) {
    if (keys.size() != values.length) {
      throw new IllegalArgumentException(
          String.format(
              "keys and values must have same size. keys.size=[%d] values.size=[%d]",
              keys.size(), values.length));
    }
    KMV kmv = new KMV(k);
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
  public void update(int hash, double value) {
    double h = Hashes.grm(hash);
    if (kMinValues.size() < maxK) {
      kMinValues.add(new ValueHash(hash, h, value));
      if (h > kthValue) {
        kthValue = h;
      }
    } else if (h < kthValue) {
      kMinValues.add(new ValueHash(hash, h, value));
      kMinValues.remove(kMinValues.last());
      kthValue = kMinValues.last().grmHash;
    }
  }

  /** The improved (unbiased) distinct value estimator (UB) from Beyer et. al., SIGMOD 2007. */
  public double distinctValues() {
    return (kMinValues.size() - 1.0) / kthValue;
  }

  /** Basic distinct value estimator (BE) from Beyer et. al., SIGMOD 2007. */
  public double distinctValuesBE() {
    return kMinValues.size() / kthValue;
  }

  /** Estimates the size of union of the given KMV synopsis */
  public double unionSize(KMV other) {
    int k = computeK(this, other);
    double kthValue = kthValueOfUnion(this.kMinValues, other.kMinValues);
    return (k - 1) / kthValue;
  }

  /** Estimates the Jaccard similarity using the p = K_e / k estimator from Beyer et. al. (2007) */
  public double jaccard(KMV other) {
    int k = computeK(this, other);
    int intersection = intersectionSize(this.kMinValues, other.kMinValues);
    double js = intersection / (double) k;
    return js;
  }

  /** Estimates intersection between the sets represeneted by this synopsis and the other. */
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
      values.add(v.grmHash);
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

  public TreeSet<ValueHash> getKMinValues() {
    return this.kMinValues;
  }

  @Override
  public String toString() {
    return "KMV{" + "maxK=" + maxK + ", kMinValues=" + kMinValues + ", kthValue=" + kthValue + '}';
  }
}
