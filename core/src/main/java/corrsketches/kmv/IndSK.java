package corrsketches.kmv;

import corrsketches.aggregations.AggregateFunction;
import corrsketches.sampling.Samplers;
import it.unimi.dsi.fastutil.doubles.DoubleArrayList;
import it.unimi.dsi.fastutil.doubles.DoubleList;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import java.util.Random;

/** Implements independent uniform sampling with fixed sample size. */
public class IndSK extends AbstractMinValueSketch<IndSK> {

  public static final int DEFAULT_K = 256;
  final int maxK;
  private final Random random;

  public IndSK(Builder builder) {
    super(builder);
    this.maxK = builder.maxSize;
    this.random = new Random();
  }

  /** Updates the sketch with the given hashed key */
  @Override
  public void update(int hash, double value) {
    final double hu = computeRank(hash, value);
    if (kMinValues.size() < maxK) {
      ValueHash vh = createOrUpdateValueHash(hash, value, hu);
      kMinValues.add(vh);
      kthValue = 1d;
      kMinItems++;
    } else if (hu <= kthValue) {
      // if the key associated with hu has been seen, we need to update existing values;
      // otherwise, we need to create a new entry and evict the largest key to make room it
      ValueHash vh = valueHashMap.get(hash);
      if (vh != null) {
        // the incoming key is already present in the sketch, just need to update it
        vh.update(value);
      } else if (hu < kthValue) {
        // This is a new unit hash we need to create a new node. Given that there will be
        // more than k minimum values, we need to evict an existing one from the heap later.
        vh = new ValueHash(hash, hu, value, aggregatorProvider.create());
        valueHashMap.put(hash, vh);
        kMinValues.add(vh);

        // Evict the greatest of the min value
        ValueHash toBeRemoved = kMinValues.last();
        kMinValues.remove(toBeRemoved);
        valueHashMap.remove(toBeRemoved.keyHash);
        kthValue = kMinValues.last().unitHash;

        // Update item counter of this key
        // Subtract the number of items of the removed ValueHash from the total number of
        // items contained in the universe sampling set (items smaller than the kth min value)
        kMinItems -= toBeRemoved.count;
      }
      kMinItems++;
    }
    seenItems++;
  }

  public double computeRank(int hash, double value) {
    return this.random.nextDouble();
  }

  @Override
  public Samples getSamples() {
    boolean uniqueKeys = isAggregate();
    final int[] keys;
    double[] values;
    if (uniqueKeys) {
      // each key is associated with only one value
      int size = kMinValues.size();
      keys = new int[size];
      values = new double[size];
      int i = 0;
      for (ValueHash vh : kMinValues) {
        keys[i] = vh.keyHash;
        values[i] = vh.value();
        i++;
      }
    } else {
      // each key is associated with multiple sampled values
      IntArrayList keyList = new IntArrayList();
      DoubleArrayList valuesList = new DoubleArrayList();
      for (ValueHash vh : kMinValues) {
        final int key = vh.keyHash;
        DoubleList aggregatorValues = vh.aggregator.values();
        // compute how many samples should be used from each sampler
        final double prob = vh.count() / (double) seenItems;
        int n = (int) Math.max(1, Math.floor(prob * maxK));
        if (n > aggregatorValues.size()) {
          n = aggregatorValues.size();
        }
        for (int i = 0; i < n; i++) {
          keyList.add(key);
          valuesList.add(aggregatorValues.getDouble(i));
        }
      }
      keys = keyList.toIntArray();
      values = valuesList.toDoubleArray();
    }
    return new Samples(keys, values, uniqueKeys);
  }

  @Override
  public double unionSize(IndSK other) {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  @Override
  public double jaccard(IndSK other) {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  @Override
  public double intersectionSize(IndSK other) {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  @Override
  public String toString() {
    return this.getClass().getName()
        + "{"
        + "maxK="
        + maxK
        + ", kMinValues="
        + kMinValues
        + ", kthValue="
        + kthValue
        + '}';
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
    public IndSK build() {
      if (this.aggregateFunction == AggregateFunction.NONE) {
        repeatedValueHandlerProvider = Samplers.reservoir(maxSize);
      } else {
        repeatedValueHandlerProvider = this.aggregateFunction.getProvider();
      }
      return new IndSK(this);
    }
  }
}
