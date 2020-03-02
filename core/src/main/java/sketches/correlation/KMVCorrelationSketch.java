package sketches.correlation;

import it.unimi.dsi.fastutil.ints.Int2DoubleMap;
import it.unimi.dsi.fastutil.ints.Int2DoubleOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntAVLTreeSet;
import it.unimi.dsi.fastutil.ints.IntSet;
import java.util.Iterator;
import java.util.List;
import java.util.TreeSet;
import sketches.kmv.IKMV;
import sketches.kmv.KMV;
import sketches.kmv.ValueHash;

public class KMVCorrelationSketch {

  private final Correlation estimator;
  private final IKMV kmv;
  private int cardinality;

  public KMVCorrelationSketch(IKMV kmv) {
    this(kmv, -1, PearsonCorrelation::coefficient);
  }

  public KMVCorrelationSketch(IKMV kmv, int cardinality, Correlation estimator) {
    this.kmv = kmv;
    this.cardinality = cardinality;
    this.estimator = estimator;
  }

  public KMVCorrelationSketch(List<String> keys, double[] values) {
    this(KMV.create(keys, values));
  }

  public KMVCorrelationSketch(List<String> keys, double[] values, int k) {
    this(KMV.create(keys, values, k));
  }

  public static KMVCorrelationSketch create(IKMV kmv) {
    return new KMVCorrelationSketch(kmv);
  }

  public static KMVCorrelationSketch create(IKMV kmv, Correlation estimator) {
    return new KMVCorrelationSketch(kmv, -1, estimator);
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
    return this.intersectionSize(other) / this.cardinality();
  }

  public double intersectionSize(KMVCorrelationSketch other) {
    return kmv.intersectionSize(other.kmv);
  }

  public CorrelationEstimate correlationTo(KMVCorrelationSketch other) {
    return correlationTo(other, this.estimator);
  }

  public CorrelationEstimate correlationTo(KMVCorrelationSketch other, Correlation estimator) {
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
    int sampleSize = commonHashes.size();
    if (sampleSize < 2) {
      // legth must be at least 2 to compute the correlation
      return new CorrelationEstimate(Double.NaN, sampleSize);
    }

    Int2DoubleMap thisMap = buildHashToValueMap(thisKMinValues);
    Int2DoubleMap otherMap = buildHashToValueMap(otherKMinValues);
    double[] thisValues = new double[sampleSize];
    double[] otherValues = new double[sampleSize];
    int i = 0;
    for (int hash : commonHashes) {
      thisValues[i] = thisMap.get(hash);
      otherValues[i] = otherMap.get(hash);
      i++;
    }
    // finally, compute correlation coefficient between common values
    double coefficient = estimator.correlation(thisValues, otherValues);

    return new CorrelationEstimate(coefficient, sampleSize);
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

  public static class CorrelationEstimate {

    public final double coefficient;
    public final int sampleSize;

    public CorrelationEstimate(final double coefficient, final int sampleSize) {
      this.coefficient = coefficient;
      this.sampleSize = sampleSize;
    }
  }
}
