package corrsketches.kmv;

import corrsketches.aggregations.AggregateFunction;
import corrsketches.util.Hashes;
import it.unimi.dsi.fastutil.doubles.DoubleArrayList;
import it.unimi.dsi.fastutil.doubles.DoubleComparators;
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import java.util.List;
import java.util.TreeSet;
import smile.sort.HeapSelect;

/** */
public class SPPKF extends AbstractMinValueSketch<SPPKF> {

  public static final int DEFAULT_K = 256;
  private final int maxK;

  ValueHash[] heapData;
  HeapSelect<ValueHash> heap;
  int actualSize;

  public SPPKF(Builder builder) {
    super(builder);
    this.maxK = builder.maxSize;
  }

  public static SPPKF.Builder builder() {
    return new SPPKF.Builder();
  }

  @Override
  public void update(int hash, double value) {
    throw new UnsupportedOperationException("This sketch does not support incremental updates.");
  }

  @Override
  public void updateAll(List<String> keys, double[] values) {
    int[] hashedKeys = new int[keys.size()];
    for (int i = 0; i < hashedKeys.length; i++) {
      hashedKeys[i] = Hashes.murmur3_32(keys.get(i));
    }
    this.updateAll(hashedKeys, values);
  }

  @Override
  public void updateAll(int[] hashedKeys, double[] values) {

    heapData = new ValueHash[maxK];
    heap = new HeapSelect<>(heapData);
    actualSize = Math.min(maxK, hashedKeys.length);

    Int2IntOpenHashMap keyCounts = new Int2IntOpenHashMap();
    for (int i = 0; i < hashedKeys.length; i++) {
      final int key = hashedKeys[i];
      final double value = values[i];

      int count = keyCounts.getOrDefault(key, 0) + 1;
      keyCounts.put(key, count);

      final int hash = Hashes.hashIntTuple(key, count);

      final double hu = Hashes.grm(hash);
      heap.add(new ValueHash(hash, hu, value, AggregateFunction.FIRST.get()));
    }
  }

  @Override
  public Samples getSamples() {
    int[] keys = new int[actualSize];
    double[] values = new double[actualSize];
    for (int i = 0; i < actualSize; i++) {
      keys[i] = heapData[i].keyHash;
      values[i] = heapData[i].value();
    }
    boolean uniqueKeys = false;
    return new Samples(keys, values, uniqueKeys);
  }

  /** Estimates the size of union of the given KMV synopsis */
  @Override
  public double unionSize(SPPKF other) {
    int k = computeK(this, other);
    double kthValue = kthValueOfUnion(this.kMinValues, other.kMinValues);
    return (k - 1) / kthValue;
  }

  /** Estimates the Jaccard similarity using the p = K_e / k estimator from Beyer et. al. (2007) */
  @Override
  public double jaccard(SPPKF other) {
    int k = computeK(this, other);
    int intersection = intersectionSize(this.kMinValues, other.kMinValues);
    return intersection / (double) k;
  }

  /** Estimates intersection between the sets represented by this synopsis and the other. */
  @Override
  public double intersectionSize(SPPKF other) {
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

  private static int computeK(SPPKF x, SPPKF y) {
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

  public static class Builder extends AbstractMinValueSketch.Builder<SPPKF> {

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
    public SPPKF build() {
      return new SPPKF(this);
    }
  }
}
