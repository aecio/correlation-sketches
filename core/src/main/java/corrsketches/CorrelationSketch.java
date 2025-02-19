package corrsketches;

import corrsketches.Table.Join;
import corrsketches.aggregations.AggregateFunction;
import corrsketches.correlation.Correlation;
import corrsketches.correlation.CorrelationType;
import corrsketches.correlation.Estimate;
import corrsketches.kmv.*;
import corrsketches.statistics.Stats;
import corrsketches.util.Hashes;
import corrsketches.util.QuickSort;
import corrsketches.util.Sorting;
import it.unimi.dsi.fastutil.ints.*;
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

  //  public CorrelationSketch(ImmutableCorrelationSketch ics) {
  //    estimator = ics.correlation;
  //    minValueSketch = new KMV()
  //    keys = ics.keys;
  //  }

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

    public double unionSize(ImmutableCorrelationSketch other) {
      int k = computeK(this, other);
      // the k-th unit hash value of the union
      double kthValue = kthOfUnion(union(this.keys, other.keys), k);
      return (k - 1) / kthValue;
    }

    /**
     * Estimates the Jaccard similarity using the p = K_e / k estimator from Beyer et. al. (2007)
     */
    public double jaccard(ImmutableCorrelationSketch other) {
      int k = computeK(this, other);
      int[] unionKeys = union(this.keys, other.keys);
      double kthValue = kthOfUnion(unionKeys, k);
      return intersectionInUnion(this, other, kthValue) / (double) k;
    }

    /** Estimates intersection between the sets represented by this synopsis and the other. */
    public double intersectionSize(ImmutableCorrelationSketch other) {
      int k = computeK(this, other);
      int[] unionKeys = union(this.keys, other.keys);
      // the k-th unit hash value of the union
      double kthValue = kthOfUnion(unionKeys, k);
      // p is an unbiased estimate of the jaccard similarity
      double p = intersectionInUnion(this, other, kthValue) / (double) k;
      double u = (k - 1) / kthValue;
      // estimation of intersection size
      return p * u;
    }

    int intersectionInUnion(
        ImmutableCorrelationSketch left, ImmutableCorrelationSketch right, double kthOfUnion) {
      int intersectionInUnionSize = 0;

      int lidx = 0;
      int ridx = 0;
      int i, j;
      while (lidx < left.keys.length && ridx < right.keys.length) {
        if (left.keys[lidx] < right.keys[ridx]) {
          lidx++;
        } else if (left.keys[lidx] > right.keys[ridx]) {
          ridx++;
        } else {
          // keys are equal,
          if (Hashes.grm(left.keys[lidx]) <= kthOfUnion) {
            intersectionInUnionSize++;
          }

          // iterate over all remaining pairs of indexes containing this key
          // TODO: simplify these loops
          //          i = lidx;
          //          j = ridx;
          //          while (i < left.keys.length && left.keys[i] == left.keys[lidx]) {
          //            j = ridx;
          //            while (j < right.keys.length && right.keys[j] == right.keys[ridx]) {
          //              j++;
          //            }
          //            i++;
          //          }
          i = lidx;
          j = ridx;
          while (i < left.keys.length && left.keys[i] == left.keys[lidx]) {
            i++;
          }
          while (j < right.keys.length && right.keys[j] == right.keys[ridx]) {
            j++;
          }
          lidx = i;
          ridx = j;
        }
      }
      return intersectionInUnionSize;
    }

    int[] union(int[] xkeys, int[] ykeys) {
      IntSet unionSet = new IntAVLTreeSet();
      for (int x : xkeys) {
        unionSet.add(x);
      }
      for (int y : ykeys) {
        unionSet.add(y);
      }
      return unionSet.toIntArray();
    }

    double kthOfUnion(int[] unionKeys, int k) {
      double[] unitHash = computeUnitHashes(unionKeys);
      Sorting.sort(
          new Sorting.Sortable() {
            @Override
            public int compare(int i, int j) {
              return Double.compare(unitHash[i], unitHash[j]);
            }

            @Override
            public void swap(int i, int j) {
              Sorting.swap(unionKeys, i, j);
              Sorting.swap(unitHash, i, j);
            }
          },
          0,
          unionKeys.length);
      return unitHash[k - 1];
    }

    private static double[] computeUnitHashes(int[] unionKeys) {
      double[] unitHash = new double[unionKeys.length];
      for (int i = 0; i < unionKeys.length; i++) {
        unitHash[i] = Hashes.grm(unionKeys[i]);
      }
      return unitHash;
    }

    private static int computeK(ImmutableCorrelationSketch x, ImmutableCorrelationSketch y) {
      int xSize = x.keys.length;
      int ySize = y.keys.length;
      int k = Math.min(xSize, ySize);
      if (k < 1) {
        throw new IllegalStateException(
            String.format(
                "Can not compute estimates on empty synopsis. x.size=[%d] y.size=[%d]",
                xSize, ySize));
      }
      return k;
    }

    public double containment(ImmutableCorrelationSketch other) {
      return Math.max(0, Math.min(1, this.intersectionSize(other) / this.cardinality()));
    }

    public double cardinality() {
      double kth = Stats.extent(computeUnitHashes(this.keys)).max;
      return (this.keys.length - 1) / kth;
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

    public Correlation estimator() {
      return estimator;
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
