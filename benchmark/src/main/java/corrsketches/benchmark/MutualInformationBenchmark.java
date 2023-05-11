package corrsketches.benchmark;

import com.fasterxml.jackson.annotation.JsonUnwrapped;
import corrsketches.CorrelationSketch;
import corrsketches.CorrelationSketch.ImmutableCorrelationSketch;
import corrsketches.CorrelationSketch.ImmutableCorrelationSketch.Join;
import corrsketches.aggregations.AggregateFunction;
import corrsketches.benchmark.Benchmark.BaseBenchmark;
import corrsketches.benchmark.CategoricalJoinAggregation.Aggregation;
import corrsketches.benchmark.CategoricalJoinAggregation.JoinStats;
import corrsketches.benchmark.MutualInformationBenchmark.Result;
import corrsketches.benchmark.datasource.MultinomialSyntheticSource.MultinomialColumnCombination;
import corrsketches.benchmark.pairwise.ColumnCombination;
import corrsketches.benchmark.pairwise.SyntheticColumnCombination;
import corrsketches.benchmark.pairwise.TablePair;
import corrsketches.benchmark.params.SketchParams;
import corrsketches.benchmark.utils.Sets;
import corrsketches.correlation.MutualInformation.MI;
import corrsketches.correlation.MutualInformationDiffEntMixed;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.stream.Collectors;

public class MutualInformationBenchmark extends BaseBenchmark<Result> {

  public static final int MINIMUM_INTERSECTION = 3; // minimum sample size for correlation is 2

  public MutualInformationBenchmark() {
    super(Result.class);
  }

  @Override
  public List<String> run(
      ColumnCombination combination,
      List<SketchParams> sketchParams,
      List<AggregateFunction> leftAggregations,
      List<AggregateFunction> rightAggregations) {

    Result result = new Result();

    TablePair tablePair = combination.getTablePair();
    ColumnPair x = tablePair.getX();
    ColumnPair y = tablePair.getY();

    if (combination instanceof SyntheticColumnCombination) {
      result.true_corr = ((SyntheticColumnCombination) combination).getCorrelation();
      result.key_dist = ((SyntheticColumnCombination) combination).getKeyDistribution();
      if (combination instanceof MultinomialColumnCombination) {
        result.multinomial_n = ((MultinomialColumnCombination) combination).getParameters().n;
      }
    }

    List<Result> groundTruthResults =
        computeFullJoinStatistics(x, y, leftAggregations, rightAggregations, result);

    List<String> results = new ArrayList<>();
    for (Result r : groundTruthResults) {
      // we don't need to report column pairs that have no intersection at all
      if (Double.isFinite(r.interxy_actual) && r.interxy_actual >= 2) {
        for (SketchParams params : sketchParams) {
          results.add(
              toCsvLine(computeSketchStatistics(r.clone(), x, y, params, r.left_agg, r.right_agg)));
        }
      }
    }

    return results;
  }

  private static List<Result> computeFullJoinStatistics(
      ColumnPair x,
      ColumnPair y,
      List<AggregateFunction> leftAggregations,
      List<AggregateFunction> rightAggregations,
      Result result) {

    HashSet<String> xKeys = new HashSet<>(x.keyValues);
    HashSet<String> yKeys = new HashSet<>(y.keyValues);
    result.cardx_actual = xKeys.size();
    result.cardy_actual = yKeys.size();
    result.interxy_actual = Sets.intersectionSize(xKeys, yKeys);
    result.unionxy_actual = Sets.unionSize(xKeys, yKeys);

    // No need compute any statistics when there is no intersection
    if (result.interxy_actual < MINIMUM_INTERSECTION) {
      return Collections.emptyList();
    }

    //    result.jcx_actual = result.interxy_actual / (double) result.cardx_actual;
    //    result.jcy_actual = result.interxy_actual / (double) result.cardy_actual;
    //    result.jsxy_actual = result.interxy_actual / (double) result.unionxy_actual;

    // correlation ground-truth after join-aggregations
    List<AggregateFunction> rightAggs =
        rightAggregations.stream()
            .filter(
                agg ->
                    agg == AggregateFunction.NONE
                        || agg.get().acceptsInputColumnType(y.columnValueType))
            .collect(Collectors.toList());

    return leftAggregations.stream()
        .filter(
            leftAgg ->
                leftAgg == AggregateFunction.NONE
                    || leftAgg.get().acceptsInputColumnType(x.columnValueType))
        .flatMap(
            leftAgg -> computeMutualInfoAfterFullJoin(x, y, leftAgg, rightAggs, result).stream())
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
      ColumnPair x,
      ColumnPair y,
      SketchParams sketchParams,
      AggregateFunction leftAggregateFn,
      AggregateFunction rightAggregateFn) {

    // create correlation sketches for the data
    CorrelationSketch sketchX = createCorrelationSketch(x, sketchParams, leftAggregateFn);
    CorrelationSketch sketchY = createCorrelationSketch(y, sketchParams, rightAggregateFn);

    ImmutableCorrelationSketch iSketchX = sketchX.toImmutable();
    ImmutableCorrelationSketch iSketchY = sketchY.toImmutable();

    Join join = iSketchX.join(iSketchY);

    // Some datasets have large column sizes, but all values can be empty strings (missing data),
    // so we need to check whether the actual cardinality and sketch sizes are large enough.
    if (result.interxy_actual >= MINIMUM_INTERSECTION && join.keys.length >= MINIMUM_INTERSECTION) {
      // computes statistics on joined data (e.g., correlations)
      estimateMutualInfoFromSketchJoin(result, join);
    }

    result.parameters = sketchParams.toString();
    result.columnId =
        String.format(
            "X(%s,%s,%s) Y(%s,%s,%s)",
            x.keyName, x.columnName, x.datasetId, y.keyName, y.columnName, y.datasetId);

    return result;
  }

  private static void estimateMutualInfoFromSketchJoin(Result result, Join join) {
    MI mi = MutualInformationDiffEntMixed.INSTANCE.of(join.x, join.y);
    result.mi_est = mi.value;
    result.join_size_sketch = mi.sampleSize;
    result.nmi_sqrt_est = mi.nmiSqrt();
    result.nmi_max_est = mi.nmiMax();
    result.nmi_min_est = mi.nmiMin();
    result.ex_est = mi.ex;
    result.ey_est = mi.ey;
    result.mi_nx_est = mi.nx;
    result.mi_ny_est = mi.ny;
  }

  public static List<Result> computeMutualInfoAfterFullJoin(
      ColumnPair columnA,
      ColumnPair columnB,
      AggregateFunction leftAggregation,
      List<AggregateFunction> rightAggregations,
      Result result) {

    List<Aggregation> joins =
        CategoricalJoinAggregation.leftJoinAggregate(columnA, columnB, rightAggregations);

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
      r.xtype = join.a.type.toString();
      r.ytype = join.b.type.toString();

      MI mi = MutualInformationDiffEntMixed.INSTANCE.of(join.a, join.b);
      r.mi_actual = mi.value;
      r.nmi_sqrt_actual = mi.nmiSqrt();
      r.nmi_max_actual = mi.nmiMax();
      r.nmi_min_actual = mi.nmiMin();
      r.ex_actual = mi.ex;
      r.ey_actual = mi.ey;
      r.join_size_actual = mi.sampleSize;
      r.mi_nx_actual = mi.nx;
      r.mi_ny_actual = mi.ny;

      results.add(r);
    }

    return results;
  }

  public static class Result implements Cloneable {

    // mutual information
    public double mi_actual;
    public double mi_est;
    // normalized mutual information variants
    public double nmi_sqrt_actual;
    public double nmi_max_actual;
    public double nmi_min_actual;
    public double nmi_sqrt_est;
    public double nmi_max_est;
    public double nmi_min_est;
    // cardinality of data vectors used to compute MI
    public int mi_nx_actual;
    public int mi_ny_actual;
    public int mi_nx_est;
    public int mi_ny_est;
    // entropy
    public double ex_actual;
    public double ey_actual;
    public double ex_est;
    public double ey_est;
    // join statistics
    public int join_size_sketch;
    public int join_size_actual;
    public int interxy_actual;
    public int unionxy_actual;
    public int cardx_actual;
    public int cardy_actual;
    // data types
    public String xtype;
    public String ytype;

    public String parameters;
    public String columnId;
    public AggregateFunction right_agg;
    public AggregateFunction left_agg;

    public float true_corr = Float.NaN;
    public String key_dist;
    public int multinomial_n;

    @JsonUnwrapped public JoinStats join_stats;

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
