package corrsketches;

import com.google.common.base.Preconditions;
import corrsketches.aggregations.AggregateFunction;
import corrsketches.correlation.Correlation;
import corrsketches.correlation.Correlation.Estimate;
import corrsketches.correlation.PearsonCorrelation;
import corrsketches.kmv.GKMV;
import corrsketches.kmv.IKMV;
import corrsketches.kmv.KMV;
import corrsketches.kmv.ValueHash;
import corrsketches.util.QuickSort;
import it.unimi.dsi.fastutil.doubles.DoubleArrayList;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.TreeSet;

public class CorrelationSketch {

  public static final Correlation DEFAULT_ESTIMATOR = PearsonCorrelation::estimate;
  public static final int UNKNOWN_CARDINALITY = -1;

  private final Correlation estimator;
  private final IKMV kmv;
  private int cardinality;

  public CorrelationSketch(IKMV kmv) {
    this(builder().sketch(kmv));
  }

  @Deprecated
  public CorrelationSketch(List<String> keys, double[] values) {
    this(builder().data(keys, values));
  }

  @Deprecated
  public CorrelationSketch(List<String> keys, double[] values, int k) {
    this(builder().data(keys, values).sketchType(SketchType.KMV, k));
  }

  private CorrelationSketch(Builder builder) {
    this.cardinality = builder.cardinality;
    this.estimator = builder.estimator;
    if (builder.sketch != null) {
      // pre-built sketch provided: just use it
      this.kmv = builder.sketch;
    } else {
      if (builder.keyValues == null && builder.columnValues == null) {
        // no data provided: build an empty sketch
        if (builder.sketchType == SketchType.KMV) {
          this.kmv = new KMV((int) builder.budget, builder.aggregateFunction);
        } else {
          this.kmv = new GKMV(builder.budget, builder.aggregateFunction);
        }
      } else {
        // data provided: initialize sketches from data
        Preconditions.checkArgument(
            builder.keyValues != null && builder.columnValues != null,
            "When data is provided, both key values and column values must be present.");
        if (builder.sketchType == SketchType.KMV) {
          this.kmv =
              KMV.create(
                  builder.keyValues,
                  builder.columnValues,
                  (int) builder.budget,
                  builder.aggregateFunction);
        } else {
          this.kmv =
              GKMV.create(
                  builder.keyValues,
                  builder.columnValues,
                  builder.budget,
                  builder.aggregateFunction);
        }
      }
    }
  }

  public static Builder builder() {
    return new Builder();
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

  public double unionSize(CorrelationSketch other) {
    return this.kmv.unionSize(other.kmv);
  }

  public double jaccard(CorrelationSketch other) {
    return kmv.jaccard(other.kmv);
  }

  public double containment(CorrelationSketch other) {
    return this.intersectionSize(other) / this.cardinality();
  }

  public double intersectionSize(CorrelationSketch other) {
    return kmv.intersectionSize(other.kmv);
  }

  public Estimate correlationTo(CorrelationSketch other) {
    return correlationTo(other, this.estimator);
  }

  public Estimate correlationTo(CorrelationSketch other, Correlation estimator) {
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

    public ImmutableCorrelationSketch(CorrelationSketch cs) {
      this.correlation = cs.estimator;
      TreeSet<ValueHash> thisKMinValues = cs.getKMinValues();
      this.keys = new int[thisKMinValues.size()];
      this.values = new double[thisKMinValues.size()];
      int i = 0;
      for (ValueHash vh : thisKMinValues) {
        keys[i] = vh.keyHash;
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
        return "Paired{"
            + "keys="
            + Arrays.toString(keys)
            + ", x="
            + Arrays.toString(x)
            + ", y="
            + Arrays.toString(y)
            + '}';
      }
    }
  }

  public static class Builder {

    protected int cardinality = UNKNOWN_CARDINALITY;
    protected Correlation estimator = DEFAULT_ESTIMATOR;
    protected AggregateFunction aggregateFunction = AggregateFunction.FIRST;

    protected SketchType sketchType = SketchType.KMV;
    protected double budget = KMV.DEFAULT_K;

    protected List<String> keyValues = null;
    protected double[] columnValues = null;

    protected IKMV sketch;

    public Builder aggregateFunction(AggregateFunction aggregateFunction) {
      this.aggregateFunction = aggregateFunction;
      return this;
    }

    public Builder data(List<String> keyValues, double[] columnValues) {
      Preconditions.checkNotNull(keyValues, "key values cannot be null");
      Preconditions.checkNotNull(columnValues, "numeric column values cannot be null");
      Preconditions.checkArgument(
          keyValues.size() == columnValues.length,
          "key values and column values must have same size");
      this.keyValues = keyValues;
      this.columnValues = columnValues;
      return this;
    }

    public Builder sketchType(SketchType sketchType, double budget) {
      this.sketchType = sketchType;
      this.budget = budget;
      return this;
    }

    public Builder sketch(IKMV sketch) {
      this.sketch = sketch;
      return this;
    }

    public Builder cardinality(int cardinality) {
      this.cardinality = cardinality;
      return this;
    }

    public Builder estimator(Correlation estimator) {
      this.estimator = estimator;
      return this;
    }

    public CorrelationSketch build() {
      return new CorrelationSketch(this);
    }
  }
}
