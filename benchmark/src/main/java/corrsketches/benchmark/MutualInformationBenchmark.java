package corrsketches.benchmark;

import com.fasterxml.jackson.annotation.JsonUnwrapped;
import corrsketches.CorrelationSketch;
import corrsketches.CorrelationSketch.ImmutableCorrelationSketch;
import corrsketches.Table.Join;
import corrsketches.aggregations.AggregateFunction;
import corrsketches.benchmark.CategoricalJoinAggregation.Aggregation;
import corrsketches.benchmark.CategoricalJoinAggregation.JoinStats;
import corrsketches.benchmark.MutualInformationBenchmark.Result;
import corrsketches.benchmark.datasource.ContDiscUnifSyntheticSource.ContUnifDiscUnifColumnCombination;
import corrsketches.benchmark.datasource.MultinomialSyntheticSource.MultinomialColumnCombination;
import corrsketches.benchmark.pairwise.ColumnCombination;
import corrsketches.benchmark.pairwise.SyntheticColumnCombination;
import corrsketches.benchmark.pairwise.TablePair;
import corrsketches.benchmark.params.SketchParams;
import corrsketches.benchmark.utils.Sets;
import corrsketches.correlation.MutualInformation.MI;
import corrsketches.correlation.MutualInformationDiffEntMixed;
import corrsketches.correlation.SpearmanCorrelation;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.stream.Collectors;

public class MutualInformationBenchmark extends BaseBenchmark<Result> {

  public static final int MINIMUM_INTERSECTION = 3; // minimum sample size for correlation is 2

  private final List<SketchParams> sketchParams;
  private final List<AggregateFunction> leftAggregations;
  private final List<AggregateFunction> rightAggregations;

  public MutualInformationBenchmark(
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
    result.cardx_actual = xKeys.size();
    result.cardy_actual = yKeys.size();
    result.interxy_actual = Sets.intersectionSize(xKeys, yKeys);
    result.unionxy_actual = Sets.unionSize(xKeys, yKeys);

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
        .flatMap(
            leftAgg -> computeMutualInfoAfterFullJoin(y, x, leftAgg, rightAggs, result).stream())
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
      Join join = iSketchY.join(iSketchX);
      result.sketch_size_x = iSketchX.getKeys().length;
      result.sketch_size_y = iSketchY.getKeys().length;
      if (join.keys.length >= MINIMUM_INTERSECTION) {
        estimateMutualInfoFromSketchJoin(result, join);
      }
    }

    result.parameters = sketchParams.toString();
    result.columnId =
        String.format(
            "Y(%s,%s,%s), X(%s,%s,%s)",
            y.keyName, y.columnName, y.datasetId, x.keyName, x.columnName, x.datasetId);

    return result;
  }

  private static void estimateMutualInfoFromSketchJoin(Result result, Join join) {
    MI mi = MutualInformationDiffEntMixed.INSTANCE.of(join.right, join.left);
    result.mi_est = mi.value;
    result.join_size_sketch = mi.sampleSize;
    result.nmi_sqrt_est = mi.nmiSqrt();
    result.nmi_max_est = mi.nmiMax();
    result.nmi_min_est = mi.nmiMin();
    result.ex_est = mi.ex;
    result.ey_est = mi.ey;
    result.mi_nx_est = mi.nx;
    result.mi_ny_est = mi.ny;
    result.corr_sp_est = SpearmanCorrelation.spearman(join.left.values, join.right.values);
    result.mi_lb_est =
        -0.5
            * (1.0
                - 0.122 * Math.pow(result.corr_sp_est, 2)
                + 0.053 * Math.pow(result.corr_sp_est, 12))
            * Math.log((1.0 - Math.pow(result.corr_sp_est, 2)));
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
      r.ytype = join.a.type.toString();
      r.xtype = join.b.type.toString();

      MI mi = MutualInformationDiffEntMixed.INSTANCE.of(join.b, join.a);
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
    public double corr_sp_est;
    public double mi_lb_est;

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
