package corrsketches.kmv;

import corrsketches.aggregations.AggregateFunction;
import corrsketches.util.Hashes;
import java.util.HashSet;
import java.util.List;
import java.util.TreeSet;

/**
 * Implements the GKMV synopsis from the paper "GB-KMV: An Augmented KMV Sketch for Approximate
 * Containment Similarity Search" by Yang et. at, ICDE, 2019.
 */
public class GKMV extends IKMV<GKMV> {

  public static final double DEFAULT_THRESHOLD = 0.1;
  private final double maxT;

  // TODO: Replace all constructors by a builder class
  @Deprecated
  public GKMV(double t) {
    this(t, AggregateFunction.FIRST);
  }

  public GKMV(double t, AggregateFunction function) {
    super(function);
    this.maxT = t;
  }

  public static GKMV create(List<String> keys, double[] values) {
    return create(keys, values, DEFAULT_THRESHOLD);
  }

  @Deprecated
  public static GKMV create(List<String> keys, double[] values, double threshold) {
    return create(keys, values, threshold, AggregateFunction.FIRST);
  }

  public static GKMV create(
      List<String> keys, double[] values, double threshold, AggregateFunction aggregateFunction) {
    GKMV gkmv = new GKMV(threshold, aggregateFunction);
    gkmv.updateAll(keys, values);
    return gkmv;
  }

  /** Creates a GKMV synopsis with threshold t from an array of hashed keys. */
  public static GKMV fromHashedKeys(int[] hashes, double[] values, double t) {
    GKMV gkmv = new GKMV(t);
    gkmv.updateAll(hashes, values);
    return gkmv;
  }

  /** Updates the GKMV synopsis with the given hashed key */
  @Override
  public void update(int hash, double value) {
    double hu = Hashes.grm(hash);
    if (hu <= maxT) {
      final ValueHash minValue = createOrUpdateValueHash(hash, value, hu);
      kMinValues.add(minValue);
      if (hu > kthValue) {
        kthValue = hu;
      }
    }
  }

  /** Estimates the size of union of the given GKMV synopsis */
  @Override
  public double unionSize(GKMV other) {
    int k = unionSize(this.kMinValues, other.kMinValues);
    double kthValue = kthValueOfUnion(other);
    return (k - 1) / kthValue;
  }

  /** Estimates the Jaccard similarity using the p = K_e / k estimator from Beyer et. al. (2007) */
  @Override
  public double jaccard(GKMV other) {
    int k = unionSize(this.kMinValues, other.kMinValues);
    int intersection = intersectionSize(this.kMinValues, other.kMinValues);
    return intersection / (double) k;
  }

  /** Estimates intersection between the sets represented by this synopsis and the other. */
  @Override
  public double intersectionSize(GKMV other) {
    int k = unionSize(this.kMinValues, other.kMinValues);
    // p is an unbiased estimate of the jaccard similarity
    double p = intersectionSize(this.kMinValues, other.kMinValues) / (double) k;
    // the k-th unit hash value of the union
    double kthValue = this.kthValueOfUnion(other);
    double u = (k - 1) / kthValue;
    // estimation of intersection size
    return p * u;
  }

  private double kthValueOfUnion(GKMV other) {
    // For GKMV, we always consider all k-min values for union. Thus, we don't need to find the
    // k-th value of the union and the k-th values is always the largest between both synopsys
    return Math.max(this.kthValue, other.kthValue);
  }

  private int unionSize(TreeSet<ValueHash> x, TreeSet<ValueHash> y) {
    // TODO: Use implementation from Sets.unionSize
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

  @Override
  public String toString() {
    return "GKMV{" + "maxT=" + maxT + ", kMinValues=" + kMinValues + ", kthValue=" + kthValue + '}';
  }
}
