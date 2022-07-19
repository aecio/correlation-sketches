package corrsketches;

import corrsketches.aggregations.AggregateFunction;
import corrsketches.correlation.Correlation;
import corrsketches.correlation.CorrelationType;
import corrsketches.correlation.Estimate;
import corrsketches.kmv.AbstractMinValueSketch;
import corrsketches.kmv.GKMV;
import corrsketches.kmv.KMV;
import corrsketches.kmv.ValueHash;
import corrsketches.util.QuickSort;
import it.unimi.dsi.fastutil.doubles.DoubleArrayList;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.TreeSet;

/**
 * Implements the Correlation Sketches algorithm described in "Santos, A., Bessa, A., Chirigati, F.,
 * Musco, C. and Freire, J., 2021, June. Correlation sketches for approximate join-correlation
 * queries. In Proceedings of the 2021 International Conference on Management of Data (pp.
 * 1531-1544)."
 */
public class CorrelationSketch {

  public static final Correlation DEFAULT_ESTIMATOR = CorrelationType.PEARSONS.get();
  public static final int UNKNOWN_CARDINALITY = -1;

  private final Correlation estimator;
  private final AbstractMinValueSketch minValueSketch;
  private final ColumnType valuesType;
  private int cardinality;

  private CorrelationSketch(Builder builder) {
    this.cardinality = builder.cardinality;
    this.estimator = builder.estimator;
    this.valuesType = builder.valuesType;
    if (builder.sketch != null) {
      // pre-built sketch provided: just use it
      this.minValueSketch = builder.sketch;
    } else {
      // build sketch with given parameters
      AbstractMinValueSketch.Builder<?> sketchBuilder;
      if (builder.sketchType == SketchType.KMV) {
        sketchBuilder = new KMV.Builder().maxSize((int) builder.budget);
      } else {
        sketchBuilder = new GKMV.Builder().threshold(builder.budget);
      }
      sketchBuilder.aggregate(builder.aggregateFunction);
      this.minValueSketch = sketchBuilder.build();
    }
  }

  public static Builder builder() {
    return new Builder();
  }

  private CorrelationSketch updateAll(List<String> keys, double[] values) {
    minValueSketch.updateAll(keys, values);
    return this;
  }

  public void setCardinality(int cardinality) {
    this.cardinality = cardinality;
  }

  public double cardinality() {
    if (this.cardinality != -1) {
      return this.cardinality;
    }
    return minValueSketch.distinctValues();
  }

  public double unionSize(CorrelationSketch other) {
    return this.minValueSketch.unionSize(other.minValueSketch);
  }

  public double jaccard(CorrelationSketch other) {
    return minValueSketch.jaccard(other.minValueSketch);
  }

  public double containment(CorrelationSketch other) {
    return this.intersectionSize(other) / this.cardinality();
  }

  public double intersectionSize(CorrelationSketch other) {
    return minValueSketch.intersectionSize(other.minValueSketch);
  }

  public Estimate correlationTo(CorrelationSketch other) {
    return correlationTo(other, this.estimator);
  }

  public Estimate correlationTo(CorrelationSketch other, Correlation estimator) {
    return toImmutable().correlationTo(other.toImmutable(), estimator);
  }

  public TreeSet<ValueHash> getKMinValues() {
    return this.minValueSketch.getKMinValues();
  }

  public ImmutableCorrelationSketch toImmutable() {
    return new ImmutableCorrelationSketch(this);
  }

  public ColumnType valuesType() {
    return valuesType;
  }

  public static class ImmutableCorrelationSketch {

    final Correlation correlation;
    final int[] keys; // sorted in ascending order
    final double[] values; // values associated with the keys
    final ColumnType valuesType; // the data type of values variable

    public ImmutableCorrelationSketch(
        int[] keys, double[] values, ColumnType valuesType, Correlation correlation) {
      this.keys = keys;
      this.values = values;
      this.valuesType = valuesType;
      this.correlation = correlation;
    }

    public ImmutableCorrelationSketch(CorrelationSketch cs) {
      this.valuesType = cs.getOutputType();
      this.correlation = cs.estimator;
      TreeSet<ValueHash> thisKMinValues = cs.getKMinValues();
      this.keys = new int[thisKMinValues.size()];
      this.values = new double[thisKMinValues.size()];
      int i = 0;
      for (ValueHash vh : thisKMinValues) {
        keys[i] = vh.keyHash;
        values[i] = vh.value();
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
      final Join join = join(other);
      return estimator.of(join.x, join.y);
    }

    public Join join(ImmutableCorrelationSketch other) {
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
      return new Join(
          k.toIntArray(),
          Column.of(x.toDoubleArray(), this.valuesType),
          Column.of(y.toDoubleArray(), other.valuesType));
    }

    public ColumnType valuesType() {
      return valuesType;
    }

    public static class Join {

      public final int[] keys;
      public final Column x;
      public final Column y;

      Join(int[] keys, Column x, Column y) {
        this.keys = keys;
        this.x = x;
        this.y = y;
      }

      @Override
      public String toString() {
        return "Join{" + "keys=" + Arrays.toString(keys) + ", x=" + x + ", y=" + y + '}';
      }
    }
  }

  public ColumnType getOutputType() {
    return this.minValueSketch.aggregateFunction().get().getOutputType(valuesType);
  }

  public static class Builder {

    protected int cardinality = UNKNOWN_CARDINALITY;
    protected Correlation estimator = DEFAULT_ESTIMATOR;
    protected AggregateFunction aggregateFunction = AggregateFunction.FIRST;
    protected SketchType sketchType = SketchType.KMV;
    protected double budget = KMV.DEFAULT_K;
    protected AbstractMinValueSketch sketch;
    protected ColumnType valuesType;

    public Builder aggregateFunction(AggregateFunction aggregateFunction) {
      this.aggregateFunction = aggregateFunction;
      return this;
    }

    public Builder sketchType(SketchType sketchType, double budget) {
      this.sketchType = sketchType;
      this.budget = budget;
      return this;
    }

    public Builder sketch(AbstractMinValueSketch sketch) {
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

    public Builder estimator(CorrelationType correlationType) {
      this.estimator(correlationType.get());
      return this;
    }

    private Builder valuesType(ColumnType valuesType) {
      this.valuesType = valuesType;
      return this;
    }

    public CorrelationSketch build() {
      return new CorrelationSketch(this);
    }

    public CorrelationSketch build(String[] keys, Column column) {
      return build(keys, column.values, column.type);
    }

    public CorrelationSketch build(List<String> keys, Column column) {
      return build(keys, column.values, column.type);
    }

    public CorrelationSketch build(String[] keys, double[] values, ColumnType valuesType) {
      return build(Arrays.asList(keys), values, valuesType);
    }

    public CorrelationSketch build(List<String> keys, double[] values, ColumnType valuesType) {
      this.valuesType(valuesType);
      return new CorrelationSketch(this).updateAll(keys, values);
    }
  }
}
