package corrsketches.benchmark;

import static com.google.common.base.Preconditions.checkArgument;

import com.fasterxml.jackson.annotation.JsonUnwrapped;
import corrsketches.Column;
import corrsketches.ColumnType;
import corrsketches.CorrelationSketch;
import corrsketches.CorrelationSketch.ImmutableCorrelationSketch;
import corrsketches.Table.Join;
import corrsketches.aggregations.AggregateFunction;
import corrsketches.benchmark.CategoricalJoinAggregation.Aggregation;
import corrsketches.benchmark.CategoricalJoinAggregation.JoinStats;
import corrsketches.benchmark.datasource.ContDiscUnifSyntheticSource.ContUnifDiscUnifColumnCombination;
import corrsketches.benchmark.datasource.MultinomialSyntheticSource.MultinomialColumnCombination;
import corrsketches.benchmark.pairwise.ColumnCombination;
import corrsketches.benchmark.pairwise.SyntheticColumnCombination;
import corrsketches.benchmark.pairwise.TablePair;
import corrsketches.benchmark.params.SketchParams;
import corrsketches.benchmark.utils.Sets;
import corrsketches.correlation.*;
import it.unimi.dsi.fastutil.doubles.DoubleArrayList;
import it.unimi.dsi.fastutil.doubles.DoubleList;
import java.util.*;
import java.util.stream.Collectors;

public class MutualInformationBenchmarkPerf
    extends BaseBenchmark<MutualInformationBenchmarkPerf.Result> {

  public static final int MINIMUM_INTERSECTION = 3; // minimum sample size for correlation is 2

  private final List<SketchParams> sketchParams;
  private final List<AggregateFunction> leftAggregations;
  private final List<AggregateFunction> rightAggregations;

  public MutualInformationBenchmarkPerf(
      List<SketchParams> sketchParams,
      List<AggregateFunction> leftAggregations,
      List<AggregateFunction> rightAggregations) {
    super(Result.class);
    this.sketchParams = sketchParams;
    this.leftAggregations = leftAggregations;
    this.rightAggregations = rightAggregations;
  }

  @Override
  public List<String> computeResults(ColumnCombination combination) {

    Result result = new Result();

    TablePair tablePair = combination.getTablePair();
    ColumnPair x = tablePair.getX();
    ColumnPair y = tablePair.getY();

    if (combination instanceof SyntheticColumnCombination) {
      result.key_dist = ((SyntheticColumnCombination) combination).getKeyDistribution();
      if (combination instanceof MultinomialColumnCombination) {
        result.true_corr = ((MultinomialColumnCombination) combination).getCorrelation();
        result.true_mi = ((MultinomialColumnCombination) combination).getMutualInformation();
        result.multinomial_n = ((MultinomialColumnCombination) combination).getParameters().n;
      }
      if (combination instanceof ContUnifDiscUnifColumnCombination) {
        result.true_mi = ((ContUnifDiscUnifColumnCombination) combination).getMutualInformation();
        result.cdunif_m = ((ContUnifDiscUnifColumnCombination) combination).getParameters().m;
      }
    }

    List<Result> groundTruthResults =
        computeFullJoinStatistics(y, x, leftAggregations, rightAggregations, result);

    List<String> results = new ArrayList<>();
    for (Result r : groundTruthResults) {
      // we don't need to report column pairs that have no intersection at all
      if (Double.isFinite(r.interxy_actual) && r.interxy_actual >= 2) {
        for (SketchParams params : sketchParams) {
          results.add(
              toCsvLine(computeSketchStatistics(r.clone(), y, x, params, r.left_agg, r.right_agg)));
        }
      }
    }

    return results;
  }

  private static List<Result> computeFullJoinStatistics(
      ColumnPair y,
      ColumnPair x,
      List<AggregateFunction> leftAggregations,
      List<AggregateFunction> rightAggregations,
      Result result) {

    HashSet<String> xKeys = new HashSet<>(x.keyValues);
    HashSet<String> yKeys = new HashSet<>(y.keyValues);
    result.interxy_actual = Sets.intersectionSize(xKeys, yKeys);

    // No need compute any statistics when there is no intersection
    if (result.interxy_actual < MINIMUM_INTERSECTION) {
      return Collections.emptyList();
    }

    // correlation ground-truth after join-aggregations
    List<AggregateFunction> rightAggs =
        rightAggregations.stream()
            .filter(agg -> agg.get().acceptsInputColumnType(x.columnValueType))
            .collect(Collectors.toList());

    return leftAggregations.stream()
        .filter(leftAgg -> leftAgg.get().acceptsInputColumnType(y.columnValueType))
        .flatMap(leftAgg -> timeMutualInfoAfterFullJoin(y, x, leftAgg, rightAggs, result).stream())
        .collect(Collectors.toList());
  }

  public static CorrelationSketch createCorrelationSketch(
      ColumnPair cp, SketchParams sketchParams, AggregateFunction function) {
    return CorrelationSketch.builder()
        .aggregateFunction(function)
        .sketchType(sketchParams.type, sketchParams.budget)
        .build(cp.keyValues, cp.columnValues, cp.columnValueType);
  }

  public static Result computeSketchStatistics(
      Result result,
      ColumnPair y,
      ColumnPair x,
      SketchParams sketchParams,
      AggregateFunction leftAggregateFn,
      AggregateFunction rightAggregateFn) {

    // create correlation sketches for the data
    CorrelationSketch sketchY = createCorrelationSketch(y, sketchParams, leftAggregateFn);
    CorrelationSketch sketchX = createCorrelationSketch(x, sketchParams, rightAggregateFn);

    ImmutableCorrelationSketch iSketchX = sketchX.toImmutable();
    ImmutableCorrelationSketch iSketchY = sketchY.toImmutable();

    // Some datasets have large column sizes, but all values can be empty strings (missing data),
    // so we need to check whether the actual cardinality and sketch sizes are large enough.
    if (result.interxy_actual >= MINIMUM_INTERSECTION) {
      // computes statistics on joined data (e.g., correlations)
      long time0 = System.nanoTime();
      Join join = iSketchY.join(iSketchX);
      result.time_join_sketch = System.nanoTime() - time0;
      result.sketch_size_x = iSketchX.getKeys().length;
      result.sketch_size_y = iSketchY.getKeys().length;
      if (join.keys.length >= MINIMUM_INTERSECTION) {
        // estimateMutualInfoFromSketchJoin(result, join);
        timeSketchEstimates(result, join);
      }
    }

    result.parameters = sketchParams.toString();
    result.columnId =
        String.format(
            "Y(%s,%s,%s), X(%s,%s,%s)",
            y.keyName, y.columnName, y.datasetId, x.keyName, x.columnName, x.datasetId);

    return result;
  }

  public static List<Result> timeMutualInfoAfterFullJoin(
      ColumnPair columnA,
      ColumnPair columnB,
      AggregateFunction leftAggregation,
      List<AggregateFunction> rightAggregations,
      Result result) {

    long time0;

    List<Aggregation> joins = timedLeftJoinAggregate(columnA, columnB, rightAggregations);

    List<Result> results = new ArrayList<>(rightAggregations.size());

    for (Aggregation join : joins) {

      // correlation is defined only for vectors of length at least two
      if (join.a.values.length < MINIMUM_INTERSECTION) {
        continue;
      }

      Result r = result.clone();
      r.left_agg = leftAggregation;
      r.right_agg = join.aggregate;
      r.join_stats = join.joinStats;
      r.ytype = join.a.type.toString();
      r.xtype = join.b.type.toString();
      r.time_join_fulljoin = join.joinStats.join_time;

      timeEstimatorsOnFullJoin(join, r);

      results.add(r);
    }

    return results;
  }

  public static List<Aggregation> timedLeftJoinAggregate(
      ColumnPair columnA, ColumnPair columnB, List<AggregateFunction> functions) {

    // create index for primary key in column B
    Map<String, DoubleArrayList> indexB = JoinAggregation.createKeyIndex(columnB);

    List<Aggregation> results = new ArrayList<>(functions.size());
    long time0;

    for (int fnIdx = 0; fnIdx < functions.size(); fnIdx++) {
      final AggregateFunction fn = functions.get(fnIdx);

      var joinStats = new JoinStats();
      time0 = System.nanoTime();

      // Join keys for column A
      List<String> joinKeysA = new ArrayList<>();

      // numeric values for column A
      DoubleList joinValuesA = new DoubleArrayList(columnA.keyValues.size());

      // numeric values for each aggregation of column B
      DoubleList joinValuesB = new DoubleArrayList();
      // compute aggregation vectors of joined values for each join key
      for (int i = 0; i < columnA.keyValues.size(); i++) {
        String keyA = columnA.keyValues.get(i);
        final double valueA = columnA.columnValues[i];
        final DoubleArrayList rowsB = indexB.get(keyA);
        if (rowsB == null || rowsB.isEmpty()) {
          joinStats.join_1to0++;
        } else {
          if (rowsB.size() == 1) {
            // 1:1 mapping
            joinStats.join_1to1++;
          } else {
            // 1:n mapping
            joinStats.join_1toN++;
          }
          joinKeysA.add(keyA);
          joinValuesA.add(valueA);
          // We need to aggregate even for 1:1 mappings, because some
          // aggregate functions may transform the original value (e.g., COUNT)
          joinValuesB.add(fn.aggregate(rowsB));
        }
      }

      joinStats.join_time = System.nanoTime() - time0;
      // In a LEFT join, the type of aggregated column B may be different from the type of the input
      // column B. (As opposed to column A, which is not aggregated, and thus keeps the same type.)
      ColumnType joinValuesBType = fn.get().getOutputType(columnB.columnValueType);
      results.add(
          new Aggregation(
              joinKeysA,
              Column.of(joinValuesA.toDoubleArray(), columnA.columnValueType),
              Column.of(joinValuesB.toDoubleArray(), joinValuesBType),
              fn,
              joinStats));
    }

    return results;
  }

  private static void timeEstimatorsOnFullJoin(Aggregation join, Result result) {
    Column y = join.a;
    Column x = join.b;
    checkArgument(x.values.length == y.values.length, "x and y must have same size");
    result.join_size_actual = x.values.length;

    long time0;
    if (x.type == y.type) { // x and y have the same type
      if (x.type == ColumnType.CATEGORICAL) {
        // both are categorical
        time0 = System.nanoTime();
        MIEstimate mi = MutualInformationMLE.mi(x.valuesAsIntArray(), y.valuesAsIntArray());
        result.time_miest_fulljoin = System.nanoTime() - time0;
        result.mi_actual = mi.value;
      }
      if (x.type == ColumnType.NUMERICAL) { // both are numerical
        time0 = System.nanoTime();
        double mi = MutualInformationMixedKSG.mi(x.values, y.values);
        result.time_miest_fulljoin = System.nanoTime() - time0;
        result.mi_actual = mi;
      }
    } else { // x and y have different types
      int[] discrete;
      double[] continuous;
      if (x.type == ColumnType.CATEGORICAL) { // and y is NUMERICAL
        discrete = x.valuesAsIntArray();
        continuous = y.values;
      } else if (x.type == ColumnType.NUMERICAL) { // and y is CATEGORICAL
        continuous = x.values;
        discrete = y.valuesAsIntArray();
      } else {
        throw new IllegalArgumentException("Variables must be either NUMERICAL or CATEGORICAL");
      }

      time0 = System.nanoTime();
      double mi = MutualInformationDC.mi(discrete, continuous);
      result.time_miest_fulljoin = System.nanoTime() - time0;
      result.mi_actual = mi;
    }
  }

  public static void timeSketchEstimates(Result result, Join join) {
    Column y = join.left;
    Column x = join.right;
    checkArgument(x.values.length == y.values.length, "x and y must have same size");
    result.join_size_sketch = x.values.length;
    //    result.join_size_sketch = mi.sampleSize;

    long time0;
    if (x.type == y.type) { // x and y have the same type
      if (x.type == ColumnType.CATEGORICAL) {
        // both are categorical
        time0 = System.nanoTime();
        MIEstimate mi = MutualInformationMLE.mi(x.valuesAsIntArray(), y.valuesAsIntArray());
        result.time_miest_sketch = System.nanoTime() - time0;
        result.mi_est = mi.value;
      }
      if (x.type == ColumnType.NUMERICAL) { // both are numerical
        time0 = System.nanoTime();
        double mi = MutualInformationMixedKSG.mi(x.values, y.values);
        result.time_miest_sketch = System.nanoTime() - time0;
        result.mi_est = mi;
      }
    } else { // x and y have different types
      int[] discrete;
      double[] continuous;
      if (x.type == ColumnType.CATEGORICAL) { // and y is NUMERICAL
        discrete = x.valuesAsIntArray();
        continuous = y.values;
      } else if (x.type == ColumnType.NUMERICAL) { // and y is CATEGORICAL
        continuous = x.values;
        discrete = y.valuesAsIntArray();
      } else {
        throw new IllegalArgumentException("Variables must be either NUMERICAL or CATEGORICAL");
      }

      time0 = System.nanoTime();
      double mi = MutualInformationDC.mi(discrete, continuous);
      result.time_miest_sketch = System.nanoTime() - time0;
      result.mi_est = mi;
    }
  }

  public static class Result implements Cloneable {
    @JsonUnwrapped public JoinStats join_stats;

    // mutual information
    public double mi_actual;
    public double mi_est;
    // join statistics
    public int join_size_sketch;
    public int join_size_actual;
    public int interxy_actual;
    // data types
    public String xtype;
    public String ytype;
    // size of sketches
    public int sketch_size_x;
    public int sketch_size_y;
    // other parameters
    public String parameters;
    public String columnId;
    public AggregateFunction right_agg;
    public AggregateFunction left_agg;
    public float true_mi = Float.NaN;
    public float true_corr = Float.NaN;
    public String key_dist;
    public int multinomial_n;
    public int cdunif_m;
    public long time_miest_sketch;
    public long time_miest_fulljoin;
    public long time_join_sketch;
    public long time_join_fulljoin;

    @Override
    public Result clone() {
      try {
        return (Result) super.clone();
      } catch (CloneNotSupportedException e) {
        throw new IllegalStateException(
            this.getClass() + " must implement the Cloneable interface.", e);
      }
    }
  }
}
