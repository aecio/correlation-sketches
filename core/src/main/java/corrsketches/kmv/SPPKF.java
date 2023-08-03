package corrsketches.kmv;

import corrsketches.aggregations.AggregateFunction;
import corrsketches.aggregations.RepeatedValueHandler;
import corrsketches.util.Hashes;
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
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
  public void updateAll(int[] hashedKeys, double[] values) {
    if (super.aggregateFunction == AggregateFunction.NONE) {
      actualSize = Math.min(maxK, hashedKeys.length); // hashedKeys may be smaller than k
      heapData = new ValueHash[maxK];
      heap = new HeapSelect<>(heapData);
      // No aggregator, so perform sampling based on the <k, count> tuple instead
      Int2IntOpenHashMap keyCounts = new Int2IntOpenHashMap();
      for (int i = 0; i < hashedKeys.length; i++) {
        final int key = hashedKeys[i];
        final double value = values[i];

        int count = keyCounts.getOrDefault(key, 0) + 1;
        keyCounts.put(key, count);

        final int tupleHash = Hashes.hashIntTuple(key, count);
        final double hu = Hashes.grm(tupleHash);

        // The NONE aggregation will only keep the first value seen,
        // and should fail if more than one element is seen
        heap.add(new ValueHash(key, hu, value, AggregateFunction.NONE.get()));
      }
    } else {
      // We need to pre-aggregate the table first
      Int2ObjectOpenHashMap<RepeatedValueHandler> aggregatorMap = new Int2ObjectOpenHashMap<>();
      RepeatedValueHandler agg;
      for (int i = 0; i < hashedKeys.length; i++) {
        agg = aggregatorMap.get(hashedKeys[i]);
        if (agg == null) {
          agg = super.aggregatorProvider.create();
          agg.first(values[i]);
          aggregatorMap.put(hashedKeys[i], agg);
        } else {
          agg.update(values[i]);
        }
      }
      int[] aggKeys = new int[aggregatorMap.size()];
      double[] aggValues = new double[aggregatorMap.size()];
      int idx = 0;
      for (Int2ObjectOpenHashMap.Entry<RepeatedValueHandler> entry :
          aggregatorMap.int2ObjectEntrySet()) {
        aggKeys[idx] = entry.getIntKey();
        aggValues[idx] = entry.getValue().aggregatedValue();
        idx++;
      }

      // Next, we find the k-minimum values to include in the sketch
      actualSize = Math.min(maxK, aggKeys.length); // hashedKeys may be smaller than k
      heapData = new ValueHash[maxK];
      heap = new HeapSelect<>(heapData);

      for (int i = 0; i < aggKeys.length; i++) {
        final int key = aggKeys[i];
        final double value = aggValues[i];

        final int tupleHash = Hashes.hashIntTuple(key, 1);
        final double hu = Hashes.grm(tupleHash);
        heap.add(new ValueHash(key, hu, value, AggregateFunction.NONE.get()));
      }
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
    boolean uniqueKeys = aggregateFunction != AggregateFunction.NONE;
    return new Samples(keys, values, uniqueKeys);
  }

  /** Estimates the size of union of the given KMV synopsis */
  @Override
  public double unionSize(SPPKF other) {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  /** Estimates the Jaccard similarity using the p = K_e / k estimator from Beyer et. al. (2007) */
  @Override
  public double jaccard(SPPKF other) {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  /** Estimates intersection between the sets represented by this synopsis and the other. */
  @Override
  public double intersectionSize(SPPKF other) {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  @Override
  public String toString() {
    return "KMV{" + "maxK=" + maxK + ", kMinValues=" + kMinValues + ", kthValue=" + kthValue + '}';
  }

  public static class Builder extends AbstractMinValueSketch.Builder<Builder> {

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
      repeatedValueHandlerProvider = this.aggregateFunction.getProvider();
      return new SPPKF(this);
    }
  }
}
