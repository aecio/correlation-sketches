package sketches.kmv;

import it.unimi.dsi.fastutil.doubles.DoubleArrayList;
import it.unimi.dsi.fastutil.doubles.DoubleComparators;
import java.util.Comparator;
import java.util.HashSet;
import java.util.TreeSet;
import sketches.correlation.Hashes;

/**
 * Implements the KMV synopsis from the paper "On Synopsis fir distinct-value estimation under
 * multiset operations" by Beyer et. at, SIGMOD, 2017.
 */
public class KMV {

  private final int maxK;
  private final TreeSet<ValueHash> kMinValues;
  private double kthValue = Double.MIN_VALUE;

  public KMV(int k) {
    this.maxK = k;
    this.kMinValues = new TreeSet<>(ValueHash.COMPARATOR_ASC);
  }

  /** Creates a KMV synopsis of size k from an array of hashed keys. */
  public static KMV fromHashedKeys(int[] hashes, double[] values, int k) {
    KMV kmv = new KMV(k);
    for (int i = 0; i < hashes.length; i++) {
      kmv.update(hashes[i], values[i]);
    }
    return kmv;
  }

  /** Updates the KMV sysnopsis with the given hashed key */
  public void update(int hash, double value) {
    double h = Hashes.grm(hash);
    if (kMinValues.size() < maxK) {
      kMinValues.add(new ValueHash(hash, h, value));
      if (h > kthValue) {
        kthValue = h;
      }
    } else if (h < kthValue) {
      kMinValues.add(new ValueHash(hash, h, value));
      kMinValues.remove(kMinValues.last());
      kthValue = kMinValues.last().grmHash;
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
  public double unionSize(KMV other) {
    int k = Math.min(this.kMinValues.size(), other.kMinValues.size());
    double kthValue = kthValueOfUnion(other);
    return (k - 1) / kthValue;
  }

  /** Estimates the Jaccard similarity using the p = K_e / k estimator from Beyer et. al. (2007) */
  public double jaccard(KMV other) {
    int intersection = intersectionSize(this.kMinValues, other.kMinValues);
    int k = Math.min(this.kMinValues.size(), other.kMinValues.size());
    double js = intersection / (double) k;
    return js;
  }

  /**
   * Estimates the jaccard containment (JC) of the set represented by this KMV into the other KVM.
   *
   * <p>JC(X, Y) = |X ∩ Y| / |X| = |this ∩ other| / |this|
   */
  public double containment(KMV other) {
    double jc = this.intersectionSize(other) / this.distinctValues();
    return jc;
  }

  /** Estimates intersection between the sets represeneted by this synopsis and the other. */
  public double intersectionSize(KMV other) {
    int k = Math.min(this.kMinValues.size(), other.kMinValues.size());
    // estimation of jaccard
    double p = intersectionSize(this.kMinValues, other.kMinValues) / (double) k;
    // estimation of union size
    double kthValue = this.kthValueOfUnion(other);
    double u = (k - 1) / kthValue;
    // estimation of intersection size
    return p * u;
  }

  private double kthValueOfUnion(KMV other) {
    TreeSet<ValueHash> union = new TreeSet<>(ValueHash.COMPARATOR_ASC);
    union.addAll(this.kMinValues);
    union.addAll(other.kMinValues);

    int maxUnionSize = this.kMinValues.size() + other.kMinValues.size();
    DoubleArrayList values = new DoubleArrayList(maxUnionSize);
    for (ValueHash v : union) {
      values.add(v.grmHash);
    }
    values.sort(DoubleComparators.NATURAL_COMPARATOR);

    int k = Math.min(this.kMinValues.size(), other.kMinValues.size());
    return values.getDouble(k - 1);
  }

  private static int intersectionSize(TreeSet<ValueHash> a, TreeSet<ValueHash> b) {
    HashSet<ValueHash> intersection = new HashSet<>(a);
    intersection.retainAll(b);
    return intersection.size();
  }

  public TreeSet<ValueHash> getKMinValues() {
    return this.kMinValues;
  }

  @Override
  public String toString() {
    return "KMV{" + "maxK=" + maxK + ", kMinValues=" + kMinValues + ", kthValue=" + kthValue + '}';
  }

  public static class ValueHash {

    private static final Comparator<ValueHash> COMPARATOR_ASC = new HashValueComparatorAscending();

    public int hashValue;
    public double grmHash;
    public double value;

    public ValueHash(int hashValue, double grmHash, double value) {
      this.hashValue = hashValue;
      this.grmHash = grmHash;
      this.value = value;
    }

    @Override
    public boolean equals(Object o) {
      return hashValue == ((ValueHash) o).hashValue;
    }

    @Override
    public int hashCode() {
      return hashValue;
    }

    @Override
    public String toString() {
      return "ValueHash{hashValue=" + hashValue + ", grmHash=" + grmHash + ", value=" + value + '}';
    }
  }

  private static class HashValueComparatorAscending implements Comparator<ValueHash> {
    @Override
    public int compare(ValueHash a, ValueHash b) {
      return Double.compare(a.grmHash, b.grmHash);
    }
  }
}
