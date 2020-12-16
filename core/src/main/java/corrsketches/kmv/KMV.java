package corrsketches.kmv;

import corrsketches.aggregations.AggregateFunction;
import corrsketches.util.Hashes;
import it.unimi.dsi.fastutil.doubles.DoubleArrayList;
import it.unimi.dsi.fastutil.doubles.DoubleComparators;
import java.util.TreeSet;

/**
 * Implements the KMV synopsis from the paper "On Synopsis for distinct-value estimation under
 * multiset operations" by Beyer et. at, SIGMOD, 2017.
 */
public class KMV extends AbstractMinValueSketch<KMV> {

  public static final int DEFAULT_K = 256;
  private final int maxK;

  // TODO: Replace all constructors by a builder class
  @Deprecated
  public KMV(int k) {
    this(k, AggregateFunction.FIRST);
  }

  @Deprecated
  public KMV(int k, AggregateFunction function) {
    super(k, function);
    if (k < 1) {
      throw new IllegalArgumentException("Minimum k size is 1, but larger is recommended.");
    }
    this.maxK = k;
  }

  public KMV(Builder builder) {
    super(builder);
    this.maxK = builder.maxSize;
  }

  public static KMV.Builder builder() {
    return new KMV.Builder();
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

  public static class Builder extends AbstractMinValueSketch.Builder<KMV> {

    private int maxSize = DEFAULT_K;

    public Builder maxSize(int maxSize) {
      if (maxSize < 1) {
        throw new IllegalArgumentException("Minimum k size is 1, but larger is recommended.");
      }
      this.maxSize = maxSize;
      return this;
    }

    @Override
    public int expectedSize() {
      return maxSize;
    }

    @Override
    public KMV build() {
      return new KMV(this);
    }
  }
}
