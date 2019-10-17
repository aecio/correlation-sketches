package sketches.kmv;

import java.util.HashSet;
import java.util.List;
import java.util.TreeSet;
import sketches.correlation.Hashes;

/**
 * Implements the GKMV synopsis from the paper "GB-KMV: An Augmented KMV Sketch for Approximate
 * Containment Similarity Search" by Yang et. at, ICDE, 2019.
 */
public class GKMV implements IKMV<GKMV> {

  public static final double DEFAULT_THRESHOLD = 0.1;

  private final double maxT;
  private final TreeSet<ValueHash> kMinValues;
  private double kthValue = Double.MIN_VALUE;

  public GKMV(double t) {
    this.maxT = t;
    this.kMinValues = new TreeSet<>(ValueHash.COMPARATOR_ASC);
  }

  public static GKMV create(List<String> keys, double[] values) {
    return create(keys, values, DEFAULT_THRESHOLD);
  }

  public static GKMV create(List<String> keys, double[] values, double threshold) {
    GKMV kmv = new GKMV(threshold);
    kmv.updateAll(keys, values);
    return kmv;
  }

  /** Creates a KMV synopsis of size k from an array of hashed keys. */
  public static GKMV fromHashedKeys(int[] hashes, double[] values, double k) {
    GKMV kmv = new GKMV(k);
    for (int i = 0; i < hashes.length; i++) {
      kmv.update(hashes[i], values[i]);
    }
    return kmv;
  }

  /** Updates the KMV sysnopsis with the given hashed key */
  public void update(int hash, double value) {
    double h = Hashes.grm(hash);
    if (h <= maxT) {
      kMinValues.add(new ValueHash(hash, h, value));
      if (h > kthValue) {
        kthValue = h;
      }
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
  public double unionSize(GKMV other) {
    int k = unionSize(this.kMinValues, other.kMinValues);
    double kthValue = kthValueOfUnion(other);
    return (k - 1) / kthValue;
  }

  /** Estimates the Jaccard similarity using the p = K_e / k estimator from Beyer et. al. (2007) */
  public double jaccard(GKMV other) {
    int intersection = intersectionSize(this.kMinValues, other.kMinValues);
    int k = unionSize(this.kMinValues, other.kMinValues);
    double js = intersection / (double) k;
    return js;
  }

  /** Estimates intersection between the sets represeneted by this synopsis and the other. */
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

  public TreeSet<ValueHash> getKMinValues() {
    return this.kMinValues;
  }

  @Override
  public String toString() {
    return "KMV{" + "maxK=" + maxT + ", kMinValues=" + kMinValues + ", kthValue=" + kthValue + '}';
  }
}
