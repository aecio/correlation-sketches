package corrsketches.kmv;

import corrsketches.sampling.BernoulliSampler;
import corrsketches.util.Hashes;
import java.util.HashSet;
import java.util.TreeSet;

/**
 * Implements the GKMV synopsis from the paper "GB-KMV: An Augmented KMV Sketch for Approximate
 * Containment Similarity Search" by Yang et. at, ICDE, 2019.
 */
public class GKMV extends AbstractMinValueSketch<GKMV> {

  public static final double DEFAULT_THRESHOLD = 0.1;
  private final double maxT;

  public GKMV(Builder builder) {
    super(builder);
    this.maxT = builder.threshold;
    // TODO: maxT = sqrt(builder.threshold) ?
  }

  public static Builder builder() {
    return new GKMV.Builder();
  }

  /** Updates the GKMV synopsis with the given hashed key */
  @Override
  public void update(int hash, double value) {
    double hu = Hashes.grm(hash);
    if (hu <= maxT) {
      final ValueHash minValue =
          createOrUpdateValueHash(hash, value, hu, new BernoulliSampler(maxT));
      kMinValues.add(minValue);
      if (hu > kthValue) {
        kthValue = hu;
      }
      // TODO: Where to sample entries?
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

  private static int unionSize(TreeSet<ValueHash> x, TreeSet<ValueHash> y) {
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

  public static class Builder extends AbstractMinValueSketch.Builder<GKMV> {

    private double threshold = DEFAULT_THRESHOLD;

    public Builder threshold(double t) {
      if (t < 0. || t > 1.) {
        throw new IllegalArgumentException(
            String.format("GKMV threshold (t=%f) must be between 0 and 1.", t));
      }
      this.threshold = t;
      return this;
    }

    @Override
    public GKMV build() {
      return new GKMV(this);
    }
  }
}
