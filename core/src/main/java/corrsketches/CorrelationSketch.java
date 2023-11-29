package corrsketches;

import corrsketches.Table.Join;
import corrsketches.aggregations.AggregateFunction;
import corrsketches.correlation.Correlation;
import corrsketches.correlation.CorrelationType;
import corrsketches.correlation.Estimate;
import corrsketches.kmv.*;
import corrsketches.util.QuickSort;
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
      } else if (builder.sketchType == SketchType.GKMV) {
        sketchBuilder = new GKMV.Builder().threshold(builder.budget);
      } else if (builder.sketchType == SketchType.TUPSK) {
        sketchBuilder = new TUPSK.Builder().maxSize((int) builder.budget);
      } else if (builder.sketchType == SketchType.PRISK) {
        sketchBuilder = new PRISK.Builder().maxSize((int) builder.budget);
      } else if (builder.sketchType == SketchType.INDSK) {
        sketchBuilder = new IndSK.Builder().maxSize((int) builder.budget);
      } else {
        throw new IllegalArgumentException("Unsupported sketch type: " + builder.sketchType);
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

  private CorrelationSketch updateAll(int[] keys, double[] values) {
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
    private boolean uniqueKeys;

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
      AbstractMinValueSketch.Samples samples = cs.minValueSketch.getSamples();
      this.keys = samples.keys;
      this.values = samples.values;
      this.uniqueKeys = samples.uniqueKeys;
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
      return estimator.of(join.left, join.right);
    }

    public Join join(ImmutableCorrelationSketch other) {
      Table left = new Table(this.keys, Column.of(this.values, this.valuesType), this.uniqueKeys);
      Table right =
          new Table(other.keys, Column.of(other.values, other.valuesType), this.uniqueKeys);
      return Table.join(left, right);
    }

    public ColumnType valuesType() {
      return valuesType;
    }
  }

  private boolean isAggregateSketch() {
    //    return this.minValueSketch.aggregateFunction() != AggregateFunction.SAMPLER;
    return this.minValueSketch.isAggregate();
  }

  public ColumnType getOutputType() {
    return this.minValueSketch.aggregatorProvider().create().getOutputType(valuesType);
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

    public CorrelationSketch build(int[] keys, Column column) {
      return build(keys, column.values, column.type);
    }

    public CorrelationSketch build(String[] keys, double[] values, ColumnType valuesType) {
      return build(Arrays.asList(keys), values, valuesType);
    }

    public CorrelationSketch build(List<String> keys, double[] values, ColumnType valuesType) {
      this.valuesType(valuesType);
      return new CorrelationSketch(this).updateAll(keys, values);
    }

    public CorrelationSketch build(int[] keys, double[] values, ColumnType valuesType) {
      this.valuesType(valuesType);
      return new CorrelationSketch(this).updateAll(keys, values);
    }
  }
}
