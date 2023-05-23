package corrsketches.kmv;

import corrsketches.aggregations.AggregateFunction;
import corrsketches.sampling.Samplers;
import corrsketches.util.Hashes;
import it.unimi.dsi.fastutil.ints.*;
import smile.sort.HeapSelect;

import java.util.Arrays;
import java.util.List;

public class WPRISK extends AbstractMinValueSketch<WPRISK> {

  public static final int DEFAULT_K = 256;
  public static final double LOG_BASE = Math.log(10);
  private final int maxK;

  ValueHash[] heapData;
  HeapSelect<ValueHash> heap;
  int actualSize;

  public WPRISK(Builder builder) {
    super(builder);
    this.maxK = builder.maxSize;
  }

  public static WPRISK.Builder builder() {
    return new WPRISK.Builder();
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

    Int2IntOpenHashMap valueCounts = new Int2IntOpenHashMap();
    Int2IntOpenHashMap keyCounts = new Int2IntOpenHashMap();
    Int2IntOpenHashMap keyValCounts = new Int2IntOpenHashMap();
    for (int i = 0; i < hashedKeys.length; i++) {
      final int key = hashedKeys[i];
      final int value = (int) values[i];
      final int keyVal = Hashes.hashIntTuple(key, value);
      keyCounts.put(key, keyCounts.getOrDefault(key, 0) + 1);
      valueCounts.put(value, valueCounts.getOrDefault(value, 0) + 1);
      keyValCounts.put(keyVal, keyValCounts.getOrDefault(keyVal, 0) + 1);
      System.out.printf("k=%d v=%d kv=%d\n", key, value, keyVal);
    }
    final int n = hashedKeys.length;

    System.out.println("\n");
    System.out.println("keyValCounts = " + keyValCounts);
    System.out.println("keyCounts = " + keyCounts);
    System.out.println("valueCounts = " + valueCounts);
    System.out.println("\n");

    //    Samples aggSamples = aggregate(hashedKeys, values);
    //    for (int i = 0; i < aggSamples.keys.length ; i++) {
    IntOpenHashSet seen = new IntOpenHashSet();
    for (int i = 0; i < hashedKeys.length; i++) {
      final int key = hashedKeys[i];
      final int value = (int) values[i];
      final int keyVal = Hashes.hashIntTuple(key, value);

      if (!seen.contains(keyVal)) {
        seen.add(keyVal);

        int keyValCount = keyValCounts.get(keyVal);

//        int valueCount = valueCounts.get(value);
//        double p = valueCount / (double) n;
        //      double ie = Math.log(1/p);
        //      double plogp = p * ie;
//        double plogp = - p * Math.log(p) / LOG_BASE;
//        double keyWeight = plogp / valueCount;

        double wi = keyValCount;

//        double wi = keyWeight * keyValCount;
//        System.out.printf(
//            "key=%d val=%d  keyval=%d  kvcount=%d p=%.3f  plogp=%.3f  keyWeight=%.3f  wi=%.3f\n",
//            key, value, keyVal, keyValCount, p, plogp, keyWeight, wi);
        this.update(keyVal, value, wi);
        System.out.printf(
                "key=%d val=%d  keyval=%d  kvcount=%d  wi=%.3f\n",
                key, value, keyVal, keyValCount, wi);
      }
    }
  }

  public void update(int hash, double value, double weight) {
    final double hu = Hashes.grm(hash) / weight;
        System.out.printf(
            "hash:%d  value:%.3f  weight:%.3f  hu:%.3f  whu:%.3f\n",
            hash, value, weight, Hashes.grm(hash), hu);
    if (kMinValues.size() < maxK) {

      ValueHash vh = createOrUpdateValueHash(hash, value, hu);
      kMinValues.add(vh);
      // if (hu > kthValue) {
      //   kthValue = hu;
      // }
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
    // System.out.println();
    // System.out.println("kMin size = " + kMinValues.size());
    // System.out.println("seenItems = " + seenItems);
    // System.out.println("kMinItems = " + kMinItems);
    // System.out.println("    ratio = " + kMinItems / (double) seenItems);
    // System.out.println("__");
  }

  @Override
  public Samples getSamples() {
    return null;
  }

  public WeightedSamples getWeightedSamples() {
    boolean uniqueKeys = isAggregate();
    final int[] keys;
    double[] values;
    double[] weights;
//    if (uniqueKeys) {
//      // each key is associated with only one value
      int size = kMinValues.size();
      keys = new int[size];
      values = new double[size];
      weights = new double[size];
      int i = 0;
      for (ValueHash vh : kMinValues) {
        keys[i] = vh.keyHash;
        values[i] = vh.value();
        weights[i] = vh.unitHash;
        i++;
      }
//    }
//    else {
//      // each key is associated with multiple sampled values
//      IntArrayList keyList = new IntArrayList();
//      DoubleArrayList valuesList = new DoubleArrayList();
//      for (ValueHash vh : kMinValues) {
//        // TODO:
//        //  1. Compute probability within the kMinValues or whole data (seenItems)?
//        //  2. Analyze if n can be greater than the number of entries in sampler for each key
//        //  (i.e., vh.aggregator.values()), and provide bounds for its max size and for \sum_i(n)
//        //  where i is each key in the sketch.
//
//        final int key = vh.keyHash;
//        DoubleList aggregatorValues = vh.aggregator.values();
//        // compute how many samples should be used from each sampler
//        // final double prob = vh.count() / (double) kMinItems;
//        final double prob = vh.count() / (double) seenItems;
//        int n = (int) Math.max(1, Math.floor(prob * maxK));
//        // System.out.printf("prob[%d] = %.4f\n", key, prob);
//        // System.out.println("------");
//        // System.out.println("key = " + key);
//        // System.out.println("seenItems = " + seenItems);
//        // System.out.println("count = " + vh.count());
//        // System.out.println("kMinItems = " + kMinItems);
//        // System.out.println("prob = " + prob);
//        // System.out.println("maxK = " + maxK);
//        // System.out.println("n = " + n);
//        if (n > aggregatorValues.size()) {
//          n = aggregatorValues.size();
//        }
//        for (int i = 0; i < n; i++) {
//          keyList.add(key);
//          valuesList.add(aggregatorValues.getDouble(i));
//        }
//      }
//      keys = keyList.toIntArray();
//      values = valuesList.toDoubleArray();
//    }
    return new WeightedSamples(keys, values, weights, uniqueKeys);
  }

  public class WeightedSamples {
    public int[] keys;
    public double[] values;
    public double[] weights;

    public boolean uniqueKeys;

    public WeightedSamples(int[] keys, double[] values, double[] weights, boolean uniqueKeys) {
      this.keys = keys;
      this.values = values;
      this.weights = weights;
      this.uniqueKeys = uniqueKeys;
    }

    @Override
    public String toString() {
      return "WeightedSamples{" +
              "keys=" + Arrays.toString(keys) +
              ", values=" + Arrays.toString(values) +
              ", weights=" + Arrays.toString(weights) +
              ", uniqueKeys=" + uniqueKeys +
              '}';
    }
  }

  public void estimateMutualInfo() {
    WeightedSamples samples = this.getWeightedSamples();
    System.out.println("samples = " + samples);
    int n = samples.keys.length;
    System.out.println();
    Int2DoubleOpenHashMap map = new Int2DoubleOpenHashMap();
    for (int i = 0; i < n; i++) {
      int key = samples.keys[i];
      double value = samples.values[i];
      double weight = 1/samples.weights[i];

      double w = map.getOrDefault((int)value, 0);
      map.put((int)value, w + weight);
      System.out.printf("key=%d  value=%d w=%.3f  ws=%.3f\n", key, (int) value, w, w+weight);
    }
    System.out.printf("\n");
    double ent = 0d;
    for (var entry : map.int2DoubleEntrySet()) {
      int key =  entry.getIntKey();
      double w =  entry.getDoubleValue();
      double p = w/11;
      double plogp = (-p * Math.log(p) / LOG_BASE);
      ent += plogp; // FIXME
      System.out.printf("key=%d  w=%.3f  -p*log(p)=%.3f \n", key, w, plogp);
    }

    System.out.println("ent = " + ent);
  }

  @Override
  public double unionSize(WPRISK other) {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  @Override
  public double jaccard(WPRISK other) {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  @Override
  public double intersectionSize(WPRISK other) {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  @Override
  public String toString() {
    return getClass().getName()
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
    public WPRISK build() {
      if (aggregateFunction == AggregateFunction.NONE) {
        this.repeatedValueHandlerProvider = Samplers.reservoir(maxSize);
      } else {
        this.repeatedValueHandlerProvider = aggregateFunction.getProvider();
      }
      return new WPRISK(this);
    }
  }
}
