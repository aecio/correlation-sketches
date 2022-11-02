package corrsketches.kmv;

import corrsketches.sampling.DoubleReservoirSampler;
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
    System.out.println("update(hash = " + hash + ", value = " + value + ")");
    final double hu = Hashes.grm(hash);
    if (kMinValues.size() < maxK) {
      System.out.printf("--> kMinValues.size() < maxK ::: %d < %d\n", kMinValues.size(), maxK);
      ValueHash vh = createOrUpdateValueHash(hash, value, hu, new DoubleReservoirSampler(maxK));
      kMinValues.add(vh);
      //      if (hu > kthValue) {
      //        kthValue = hu;
      //      }
      kthValue = 1d;
      kMinItems++;
    } else if (hu <= kthValue) {
      // if the key associated with hu has been seen, we need to update existing values;
      // otherwise, we need to create a new entry and evict the largest key to make room it
      System.out.printf("--> hu < kthValue  ::: %.4f < %.4f\n", hu, kthValue);

      ValueHash vh = valueHashMap.get(hash);
      if (vh == null && hu < kthValue) {
        // This is a new unit hash we need to create a new node. Given that there will be
        // more than k minimum values, we need to evict an existing one from the heap later.
        vh = new ValueHash(hash, hu, value, function, new DoubleReservoirSampler(maxK));
        valueHashMap.put(hash, vh);
        kMinValues.add(vh);

        // Evict the greatest of the min value
        ValueHash toBeRemoved = kMinValues.last();
        kMinValues.remove(toBeRemoved);
        valueHashMap.remove(toBeRemoved.keyHash);
        kthValue = kMinValues.last().unitHash;
        System.out.println(
            "adding k="
                + vh.keyHash
                + ", evicting k="
                + toBeRemoved.keyHash
                + " count="
                + toBeRemoved.count);

        // Update item counter of this key
        // Subtract the number of items of the removed ValueHash from the total number of
        // items contained in the universe sampling set (items smaller than the kth min value)
        kMinItems -= toBeRemoved.count;
      } else {
        vh.update(value);
      }
      kMinItems++;
      // END

      System.out.printf("k[%d] count = %d\n", vh.keyHash, vh.count);

      // TODO:
      //  (1) User reservoir sampling to select some items;
      //  (2) remove items form the sample whenever we remove a value hash from the k-min values
      //      (issue: how to make sure probabilities are uniform?)

      // update BS

      // update
    }
    seenItems++;

    System.out.println();
    System.out.println("kMin size = " + kMinValues.size());
    System.out.println("seenItems = " + seenItems);
    System.out.println("kMinItems = " + kMinItems);
    System.out.println("    ratio = " + kMinItems / (double) seenItems);
    System.out.println("__");
  }

  class T3 {
    int key;
    double value;
    double unitHash;

    T3(int key, double unitHash, double value) {
      this.key = key;
      this.unitHash = unitHash;
      this.value = value;
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
