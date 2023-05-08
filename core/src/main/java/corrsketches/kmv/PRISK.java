package corrsketches.kmv;

import corrsketches.util.Hashes;
import it.unimi.dsi.fastutil.doubles.DoubleArrayList;
import it.unimi.dsi.fastutil.doubles.DoubleComparators;
import it.unimi.dsi.fastutil.doubles.DoubleList;
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import java.util.List;
import java.util.TreeSet;
import smile.sort.HeapSelect;

public class PRISK extends AbstractMinValueSketch<PRISK> {

  public static final int DEFAULT_K = 256;
  private final int maxK;

  ValueHash[] heapData;
  HeapSelect<ValueHash> heap;
  int actualSize;

  public PRISK(Builder builder) {
    super(builder);
    this.maxK = builder.maxSize;
  }

  public static PRISK.Builder builder() {
    return new PRISK.Builder();
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
      int count = keyCounts.getOrDefault(key, 0) + 1;
      keyCounts.put(key, count);
    }

    for (int i = 0; i < hashedKeys.length; i++) {
      final int key = hashedKeys[i];
      final double value = values[i];

      int count = keyCounts.getOrDefault(key, 0);
      if (count == 0) {
        throw new IllegalStateException();
      }

      this.update(key, value, count);
    }
  }

  public void update(int hash, double value, double weight) {
    final double hu = Hashes.grm(hash) / weight;
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
        // TODO:
        //  1. Compute probability within the kMinValues or whole data (seenItems)?
        //  2. Analyze if n can be greater than the number of entries in sampler for each key
        //  (i.e., vh.aggregator.values()), and provide bounds for its max size and for \sum_i(n)
        //  where i is each key in the sketch.

        final int key = vh.keyHash;
        DoubleList aggregatorValues = vh.aggregator.values();
        // compute how many samples should be used from each sampler
        // final double prob = vh.count() / (double) kMinItems;
        final double prob = vh.count() / (double) seenItems;
        int n = (int) Math.max(1, Math.floor(prob * maxK));
        //         System.out.printf("prob[%d] = %.4f\n", key, prob);
        // System.out.println("------");
        // System.out.println("key = " + key);
        // System.out.println("seenItems = " + seenItems);
        // System.out.println("count = " + vh.count());
        // System.out.println("kMinItems = " + kMinItems);
        // System.out.println("prob = " + prob);
        // System.out.println("maxK = " + maxK);
        // System.out.println("n = " + n);
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

  /** Estimates the size of union of the given KMV synopsis */
  @Override
  public double unionSize(PRISK other) {
    int k = computeK(this, other);
    double kthValue = kthValueOfUnion(this.kMinValues, other.kMinValues);
    return (k - 1) / kthValue;
  }

  /** Estimates the Jaccard similarity using the p = K_e / k estimator from Beyer et. al. (2007) */
  @Override
  public double jaccard(PRISK other) {
    int k = computeK(this, other);
    int intersection = intersectionSize(this.kMinValues, other.kMinValues);
    return intersection / (double) k;
  }

  /** Estimates intersection between the sets represented by this synopsis and the other. */
  @Override
  public double intersectionSize(PRISK other) {
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

  private static int computeK(PRISK x, PRISK y) {
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

  public static class Builder extends AbstractMinValueSketch.Builder<PRISK> {

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
    public PRISK build() {
      return new PRISK(this);
    }
  }
}
