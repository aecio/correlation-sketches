package sketches.correlation;

import it.unimi.dsi.fastutil.ints.Int2DoubleMap;
import it.unimi.dsi.fastutil.ints.Int2DoubleOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntAVLTreeSet;
import it.unimi.dsi.fastutil.ints.IntSet;
import java.util.Iterator;
import java.util.List;
import java.util.TreeSet;
import sketches.kmv.KMV;
import sketches.kmv.KMV.ValueHash;

public class KMVCorrelationSketch {

  private static final int DEFAULT_K = 256;

  private final KMV kmv;
  private int cardinality = -1;

  public KMVCorrelationSketch(List<String> keys, double[] values) {
    this(keys, values, DEFAULT_K);
  }

  public KMVCorrelationSketch(List<String> keys, double[] values, int k) {
    this.kmv = new KMV(k);
    this.updateAll(keys, values);
  }

  public KMVCorrelationSketch(KMV kmv) {
    this(kmv, -1);
  }

  public KMVCorrelationSketch(KMV kmv, int cardinality) {
    this.kmv = kmv;
    this.cardinality = cardinality;
  }

  public static KMVCorrelationSketch fromStringHashes(String[] hashes, double[] values) {
    if (hashes.length != values.length) {
      throw new IllegalArgumentException(
          "Number of values cannot be different from number of hashes");
    }
    KMV kmv = new KMV(hashes.length);
    for (int i = 0; i < hashes.length; i++) {
      kmv.update(Integer.parseInt(hashes[i]), values[i]);
    }
    return new KMVCorrelationSketch(kmv);
  }

  public void updateAll(List<String> keys, double[] values) {
    if (keys.size() != values.length) {
      throw new IllegalArgumentException("keys and values must have equal size.");
    }
    for (int i = 0; i < values.length; i++) {
      update(keys.get(i), values[i]);
    }
  }

  public void update(String key, double value) {
    if (key == null || key.isEmpty()) {
      return;
    }
    int keyHash = Hashes.murmur3_32(key);
    kmv.update(keyHash, value);
  }

  public void setCardinality(int cardinality) {
    this.cardinality = cardinality;
  }

  public double cardinality() {
    if (this.cardinality != -1) {
      return this.cardinality;
    }
    return kmv.distinctValues();
  }

  public double unionSize(KMVCorrelationSketch other) {
    return this.kmv.unionSize(other.kmv);
  }

  public double jaccard(KMVCorrelationSketch other) {
    return kmv.jaccard(other.kmv);
  }

  public double containment(KMVCorrelationSketch other) {
    return kmv.containment(other.kmv);
  }

  public double intersectionSize(KMVCorrelationSketch other) {
    return kmv.intersectionSize(other.kmv);
  }

  public double correlationTo(KMVCorrelationSketch other) {
    TreeSet<ValueHash> thisKMinValues = this.kmv.getKMinValues();
    int[] thisMinhashes = new int[thisKMinValues.size()];
    Iterator<ValueHash> thisIt = thisKMinValues.iterator();
    for (int i = 0; i < thisMinhashes.length; i++) {
      thisMinhashes[i] = thisIt.next().hashValue;
    }

    TreeSet<ValueHash> otherKMinValues = other.kmv.getKMinValues();
    int[] otherMinhashes = new int[otherKMinValues.size()];
    Iterator<ValueHash> otherIt = otherKMinValues.iterator();
    for (int i = 0; i < otherMinhashes.length; i++) {
      otherMinhashes[i] = otherIt.next().hashValue;
    }

    // compute intersection between both sketches
    IntSet commonHashes = commonValues(thisMinhashes, otherMinhashes);
    if (commonHashes.isEmpty()) {
      return Double.NaN;
    }

    Int2DoubleMap thisMap = buildHashToValueMap(thisKMinValues);
    Int2DoubleMap otherMap = buildHashToValueMap(otherKMinValues);
    double[] thisValues = new double[commonHashes.size()];
    double[] otherValues = new double[commonHashes.size()];
    int i = 0;
    for (int hash : commonHashes) {
      thisValues[i] = thisMap.get(hash);
      otherValues[i] = otherMap.get(hash);
      i++;
    }
    // finally, compute correlation coefficient between common values
    return PearsonCorrelation.coefficient(thisValues, otherValues);
  }

  private Int2DoubleMap buildHashToValueMap(TreeSet<ValueHash> kMinValues) {
    Int2DoubleMap map = new Int2DoubleOpenHashMap();
    for (ValueHash vh : kMinValues) {
      map.putIfAbsent(vh.hashValue, vh.value);
    }
    return map;
  }

  private IntAVLTreeSet commonValues(int[] thisMinhashes, int[] otherMinhashes) {
    IntAVLTreeSet commonHashes = new IntAVLTreeSet(thisMinhashes);
    commonHashes.retainAll(new IntAVLTreeSet(otherMinhashes));
    return commonHashes;
  }

  public TreeSet<ValueHash> getKMinValues() {
    return this.kmv.getKMinValues();
  }
}
