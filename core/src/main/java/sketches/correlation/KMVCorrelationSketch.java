package sketches.correlation;

import it.unimi.dsi.fastutil.doubles.DoubleArrayList;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.TreeSet;
import sketches.correlation.Correlation.Estimate;
import sketches.kmv.IKMV;
import sketches.kmv.KMV;
import sketches.kmv.ValueHash;
import sketches.util.QuickSort;

public class KMVCorrelationSketch {

  private final Correlation estimator;
  private final IKMV kmv;
  private int cardinality;

  public KMVCorrelationSketch(IKMV kmv) {
    this(kmv, -1, PearsonCorrelation::estimate);
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

  public Estimate correlationTo(KMVCorrelationSketch other) {
    return correlationTo(other, this.estimator);
  }

  public Estimate correlationTo(KMVCorrelationSketch other, Correlation estimator) {
    return toImmutable().correlationTo(other.toImmutable(), estimator);
  }

  public TreeSet<ValueHash> getKMinValues() {
    return this.kmv.getKMinValues();
  }

  public ImmutableCorrelationSketch toImmutable() {
    return new ImmutableCorrelationSketch(this);
  }

  public static class ImmutableCorrelationSketch {

    Correlation correlation;

    int[] keys; // sorted in ascending order
    double[] values; // values associated with the keys

    public ImmutableCorrelationSketch(int[] keys, double[] values, Correlation correlation) {
      this.keys = keys;
      this.values = values;
      this.correlation = correlation;
    }

    public ImmutableCorrelationSketch(KMVCorrelationSketch cs) {
      this.correlation = cs.estimator;
      TreeSet<ValueHash> thisKMinValues = cs.getKMinValues();
      this.keys = new int[thisKMinValues.size()];
      this.values = new double[thisKMinValues.size()];
      int i = 0;
      for (ValueHash vh : thisKMinValues) {
        keys[i] = vh.hashValue;
        values[i] = vh.value;
        i++;
      }
      QuickSort.sort(keys, values);
    }

    public int[] getKeys() {
      return keys;
    }

    public double[] getValues() {
      return values;
    }

    public Estimate correlationTo(ImmutableCorrelationSketch other) {
      return correlationTo(other, correlation);
    }

    public Estimate correlationTo(ImmutableCorrelationSketch other, Correlation estimator) {
      final Paired paired = intersection(other);
      return estimator.correlation(paired.x, paired.y);
    }

    public Paired intersection(ImmutableCorrelationSketch other) {
      final int capacity = Math.max(this.keys.length, other.keys.length);
      IntArrayList k = new IntArrayList(capacity);
      DoubleArrayList x = new DoubleArrayList(capacity);
      DoubleArrayList y = new DoubleArrayList(capacity);
      int xidx = 0;
      int yidx = 0;
      while (xidx < this.keys.length && yidx < other.keys.length) {
        if (this.keys[xidx] < other.keys[yidx]) {
          xidx++;
        } else if (this.keys[xidx] > other.keys[yidx]) {
          yidx++;
        } else {
          // keys are equal
          k.add(this.keys[xidx]);
          x.add(this.values[xidx]);
          y.add(other.values[yidx]);
          xidx++;
          yidx++;
        }
      }
      return new Paired(k.toIntArray(), x.toDoubleArray(), y.toDoubleArray());
    }

    public static class Paired {

      public final int[] keys;
      public final double[] x;
      public final double[] y;

      Paired(int[] keys, double[] x, double[] y) {
        this.keys = keys;
        this.x = x;
        this.y = y;
      }

      @Override
      public String toString() {
        return "Paired{" +
            "keys=" + Arrays.toString(keys) +
            ", x=" + Arrays.toString(x) +
            ", y=" + Arrays.toString(y) +
            '}';
      }
    }
  }
}
