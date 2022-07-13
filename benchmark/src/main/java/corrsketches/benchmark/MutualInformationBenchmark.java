package corrsketches.benchmark;

import com.fasterxml.jackson.annotation.JsonUnwrapped;
import corrsketches.CorrelationSketch;
import corrsketches.CorrelationSketch.ImmutableCorrelationSketch;
import corrsketches.CorrelationSketch.ImmutableCorrelationSketch.Join;
import corrsketches.aggregations.AggregateFunction;
import corrsketches.benchmark.Benchmark.BaseBenchmark;
import corrsketches.benchmark.CategoricalJoinAggregation.Aggregation;
import corrsketches.benchmark.CategoricalJoinAggregation.JoinStats;
import corrsketches.benchmark.ComputePairwiseJoinCorrelations.SketchParams;
import corrsketches.benchmark.MutualInformationBenchmark.Result;
import corrsketches.benchmark.utils.Sets;
import corrsketches.correlation.Estimate;
import corrsketches.correlation.MutualInformation;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;

public class MutualInformationBenchmark extends BaseBenchmark<Result> {

  public static final int MINIMUM_INTERSECTION = 3; // minimum sample size for correlation is 2

  public MutualInformationBenchmark() {
    super(Result.class);
  }

  public List<String> run(
      ColumnPair x,
      ColumnPair y,
      List<SketchParams> sketchParams,
      List<AggregateFunction> functions) {

    List<Result> groundTruthResults = computeFullJoinStatistics(x, y, functions);

    List<String> results = new ArrayList<>();
    for (Result result : groundTruthResults) {
      // we don't need to report column pairs that have no intersection at all
      if (Double.isFinite(result.interxy_actual) && result.interxy_actual >= 2) {
        for (SketchParams params : sketchParams) {
          var r = computeSketchStatistics(result.clone(), x, y, params, result.aggregate);
          results.add(toCsvLine(r));
        }
      }
    }

    return results;
  }

  private static List<Result> computeFullJoinStatistics(
      ColumnPair x, ColumnPair y, List<AggregateFunction> functions) {

    Result result = new Result();

    HashSet<String> xKeys = new HashSet<>(x.keyValues);
    HashSet<String> yKeys = new HashSet<>(y.keyValues);
    result.cardx_actual = xKeys.size();
    result.cardy_actual = yKeys.size();
    result.interxy_actual = Sets.intersectionSize(xKeys, yKeys);

    // No need compute any statistics when there is no intersection
    if (result.interxy_actual < MINIMUM_INTERSECTION) {
      return Collections.emptyList();
    }

    //    result.unionxy_actual = Sets.unionSize(xKeys, yKeys);
    //    result.jcx_actual = result.interxy_actual / (double) result.cardx_actual;
    //    result.jcy_actual = result.interxy_actual / (double) result.cardy_actual;
    //    result.jsxy_actual = result.interxy_actual / (double) result.unionxy_actual;

    // correlation ground-truth after join-aggregations
    return computeMutualInfoAfterJoin(x, y, functions, result);
  }

  public static CorrelationSketch createCorrelationSketch(
      ColumnPair cp, SketchParams sketchParams, AggregateFunction function) {
    return CorrelationSketch.builder()
        .aggregateFunction(function)
        .sketchType(sketchParams.type, sketchParams.budget)
        .build(cp.keyValues, cp.columnValues);
  }

  public static Result computeSketchStatistics(
      Result result,
      ColumnPair x,
      ColumnPair y,
      SketchParams sketchParams,
      AggregateFunction function) {

    // create correlation sketches for the data
    CorrelationSketch sketchX = createCorrelationSketch(x, sketchParams, function);
    CorrelationSketch sketchY = createCorrelationSketch(y, sketchParams, function);

    ImmutableCorrelationSketch iSketchX = sketchX.toImmutable();
    ImmutableCorrelationSketch iSketchY = sketchY.toImmutable();

    Join join = iSketchX.join(iSketchY);

    // Some datasets have large column sizes, but all values can be empty strings (missing data),
    // so we need to check whether the actual cardinality and sketch sizes are large enough.
    if (result.interxy_actual >= MINIMUM_INTERSECTION
        && join.keys.length >= MINIMUM_INTERSECTION) {
      // computes statistics on joined data (e.g., correlations)
      estimateMutualInfo(result, join);
    }

    result.parameters = sketchParams.toString();
    result.columnId =
        String.format(
            "X(%s,%s,%s) Y(%s,%s,%s)",
            x.keyName, x.columnName, x.datasetId, y.keyName, y.columnName, y.datasetId);

    return result;
  }

  private static void estimateMutualInfo(Result result, Join paired) {
    result.join_size_sketch = paired.keys.length;

    Estimate mi = MutualInformation.estimate(paired.x, paired.y);
    result.mi_est = mi.value;
    result.mi_delta = result.mi_actual - result.mi_est;
  }

  public static List<Result> computeMutualInfoAfterJoin(
      ColumnPair columnA, ColumnPair columnB, List<AggregateFunction> functions, Result result) {

    List<Aggregation> joins =
        CategoricalJoinAggregation.leftJoinAggregate(columnA, columnB, functions);

    List<Result> results = new ArrayList<>(functions.size());

    for (Aggregation join : joins) {
      double[] joinedA = join.valuesA;
      double[] joinedB = join.valuesB;

      // correlation is defined only for vectors of length at least two
      if (joinedA.length < MINIMUM_INTERSECTION) {
        continue;
      }

      Result r = result.clone();
      r.aggregate = join.aggregate;
      r.join_stats = join.joinStats;

      Estimate mi = MutualInformation.estimate(joinedA, joinedB);
      r.mi_actual = mi.value;
      r.join_size_actual = mi.sampleSize;
      results.add(r);
    }

    return results;
  }

  public static class Result implements Cloneable {

    public double mi_actual;
    public double mi_est;
    public double mi_delta;
    public int join_size_sketch;

    public int join_size_actual;
    public int interxy_actual;
    public int cardx_actual;
    public int cardy_actual;

    public String parameters;
    public String columnId;
    public AggregateFunction aggregate;

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
