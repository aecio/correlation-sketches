package corrsketches.benchmark;

import corrsketches.CorrelationSketch;
import corrsketches.CorrelationSketch.ImmutableCorrelationSketch;
import corrsketches.CorrelationSketch.ImmutableCorrelationSketch.Join;
import corrsketches.aggregations.AggregateFunction;
import corrsketches.benchmark.params.SketchParams;
import corrsketches.benchmark.JoinAggregation.NumericJoinAggregation;
import corrsketches.benchmark.PerfResult.ComputingTime;
import corrsketches.benchmark.utils.Sets;
import corrsketches.correlation.BootstrapedPearson;
import corrsketches.correlation.BootstrapedPearson.BootstrapEstimate;
import corrsketches.correlation.Estimate;
import corrsketches.correlation.PearsonCorrelation;
import corrsketches.correlation.QnCorrelation;
import corrsketches.correlation.RinCorrelation;
import corrsketches.correlation.SpearmanCorrelation;
import corrsketches.statistics.Kurtosis;
import corrsketches.statistics.Stats;
import corrsketches.statistics.Stats.Extent;
import corrsketches.statistics.Variance;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;

public class CorrelationStatsBenchmark implements Benchmark {

  public static final int minimumIntersection = 3; // minimum sample size for correlation is 2

  @Override
  public List<String> run(
      ColumnPair x,
      ColumnPair y,
      List<SketchParams> sketchParams,
      List<AggregateFunction> functions) {
    return computeStatistics(x, y, sketchParams, functions);
  }

  @Override
  public String csvHeader() {
    return MetricsResult.csvHeader();
  }

  public static List<String> computeStatistics(
      ColumnPair x,
      ColumnPair y,
      List<SketchParams> sketchParams,
      List<AggregateFunction> functions) {

    List<MetricsResult> groundTruthResults = computeFullJoinStatistics(x, y, functions);

    List<String> results = new ArrayList<>();
    for (MetricsResult result : groundTruthResults) {
      // we don't need to report column pairs that have no intersection at all
      if (Double.isFinite(result.interxy_actual) && result.interxy_actual >= 2) {
        for (SketchParams params : sketchParams) {
          var r = computeSketchStatistics(result.clone(), x, y, params, result.aggregate);
          results.add(r.csvLine() + "\n");
        }
      }
    }

    return results;
  }

  private static List<MetricsResult> computeFullJoinStatistics(
      ColumnPair x, ColumnPair y, List<AggregateFunction> functions) {

    MetricsResult result = new MetricsResult();

    HashSet<String> xKeys = new HashSet<>(x.keyValues);
    HashSet<String> yKeys = new HashSet<>(y.keyValues);
    result.cardx_actual = xKeys.size();
    result.cardy_actual = yKeys.size();
    result.interxy_actual = Sets.intersectionSize(xKeys, yKeys);

    // No need compute any statistics when there is no intersection
    if (result.interxy_actual < minimumIntersection) {
      return Collections.emptyList();
    }

    result.unionxy_actual = Sets.unionSize(xKeys, yKeys);
    result.jcx_actual = result.interxy_actual / (double) result.cardx_actual;
    result.jcy_actual = result.interxy_actual / (double) result.cardy_actual;
    result.jsxy_actual = result.interxy_actual / (double) result.unionxy_actual;

    // statistics derived from the original numeric data columns
    computeNumericColumnStatistics(x, y, result);

    // correlation ground-truth after join-aggregations
    return computeCorrelationsAfterJoin(x, y, functions, result);
  }

  private static void computeNumericColumnStatistics(
      ColumnPair x, ColumnPair y, MetricsResult result) {

    result.kurtx_g2_actual = Kurtosis.g2(x.columnValues);
    result.kurty_g2_actual = Kurtosis.g2(y.columnValues);

    final Extent extentX = Stats.extent(x.columnValues);
    result.x_min = extentX.min;
    result.x_max = extentX.max;

    final Extent extentY = Stats.extent(y.columnValues);
    result.y_min = extentY.min;
    result.y_max = extentY.max;
  }

  public static CorrelationSketch createCorrelationSketch(
      ColumnPair cp, SketchParams sketchParams, AggregateFunction function) {
    return CorrelationSketch.builder()
        .aggregateFunction(function)
        .sketchType(sketchParams.type, sketchParams.budget)
        .build(cp.keyValues, cp.columnValues, cp.columnValueType);
  }

  public static MetricsResult computeSketchStatistics(
      MetricsResult result,
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
    // so we need to check weather the actual cardinality and sketch sizes are large enough.
    if (result.interxy_actual >= minimumIntersection && join.keys.length >= minimumIntersection) {

      // set operations estimates (jaccard, cardinality, etc)
      computeSetStatisticsEstimates(result, sketchX, sketchY);

      // computes statistics on joined data (e.g., correlations)
      computeSketchJoinStatistics(result, join);
    }

    result.parameters = sketchParams.toString();
    result.columnId =
        String.format(
            "X(%s,%s,%s) Y(%s,%s,%s)",
            x.keyName, x.columnName, x.datasetId, y.keyName, y.columnName, y.datasetId);

    return result;
  }

  private static void computeSketchJoinStatistics(MetricsResult result, Join join) {

    // Sample size used to estimate correlations
    result.corr_est_sample_size = join.keys.length;

    // correlation estimates
    Estimate estimate = PearsonCorrelation.estimate(join.x.values, join.y.values);
    result.corr_rp_est = estimate.value;
    result.corr_rp_delta = result.corr_rp_actual - result.corr_rp_est;

    //    if (estimate.sampleSize > 2) {
    //      // statistical significance is only defined for sample size > 2
    //      int sampleSize = estimate.sampleSize;
    //      result.corr_rp_est_pvalue2t = PearsonCorrelation.pValueTwoTailed(result.corr_rp_est,
    // sampleSize);
    //
    //      double alpha = .05;
    //      result.corr_rp_est_fisher =
    //          PearsonCorrelation.confidenceInterval(result.corr_rp_est, sampleSize, 1. - alpha);
    //      result.corr_est_significance =
    //          PearsonCorrelation.isSignificant(result.corr_rp_est, sampleSize, alpha);
    //    }

    Estimate qncorr = QnCorrelation.estimate(join.x.values, join.y.values);
    result.corr_rqn_est = qncorr.value;
    result.corr_rqn_delta = result.corr_rqn_actual - result.corr_rqn_est;

    Estimate corrSpearman = SpearmanCorrelation.estimate(join.x.values, join.y.values);
    result.corr_rs_est = corrSpearman.value;
    result.corr_rs_delta = result.corr_rs_actual - result.corr_rs_est;

    Estimate corrRin = RinCorrelation.estimate(join.x.values, join.y.values);
    result.corr_rin_est = corrRin.value;
    result.corr_rin_delta = result.corr_rin_actual - result.corr_rin_est;

    BootstrapEstimate corrPm1 = BootstrapedPearson.estimate(join.x.values, join.y.values);
    result.corr_pm1_mean = corrPm1.corrBsMean;
    result.corr_pm1_mean_delta = result.corr_rp_actual - result.corr_pm1_mean;

    result.corr_pm1_median = corrPm1.corrBsMedian;
    result.corr_pm1_median_delta = result.corr_rp_actual - result.corr_pm1_median;

    result.corr_pm1_lb = corrPm1.lowerBound;
    result.corr_pm1_ub = corrPm1.upperBound;

    // Kurtosis of variables after the join
    result.kurtx_g2 = Kurtosis.g2(join.x.values);
    result.kurtx_G2 = Kurtosis.G2(join.x.values);
    result.kurtx_k5 = Kurtosis.k5(join.x.values);
    result.kurty_g2 = Kurtosis.G2(join.y.values);
    result.kurty_G2 = Kurtosis.G2(join.y.values);
    result.kurty_k5 = Kurtosis.k5(join.y.values);

    final Extent extentX = Stats.extent(join.x.values);
    result.x_min_sample = extentX.min;
    result.x_max_sample = extentX.max;

    final Extent extentY = Stats.extent(join.y.values);
    result.y_min_sample = extentY.min;
    result.y_max_sample = extentY.max;

    double[] unitRangeX = Stats.unitize(join.x.values, result.x_min, result.x_max);
    double[] unitRangeY = Stats.unitize(join.y.values, result.y_min, result.y_max);
    result.x_sample_mean = Stats.mean(unitRangeX);
    result.y_sample_mean = Stats.mean(unitRangeY);
    result.x_sample_var = Variance.uvar(unitRangeX);
    result.y_sample_var = Variance.uvar(unitRangeY);
    result.nu_xy = Stats.dotn(unitRangeX, unitRangeY);
    result.nu_x = Stats.dotn(unitRangeX, unitRangeX);
    result.nu_y = Stats.dotn(unitRangeY, unitRangeY);
  }

  private static void computeSetStatisticsEstimates(
      MetricsResult result, CorrelationSketch sketchX, CorrelationSketch sketchY) {
    result.jcx_est = sketchX.containment(sketchY);
    result.jcy_est = sketchY.containment(sketchX);
    result.jsxy_est = sketchX.jaccard(sketchY);
    result.cardx_est = sketchX.cardinality();
    result.cardy_est = sketchY.cardinality();
    result.interxy_est = sketchX.intersectionSize(sketchY);
    result.unionxy_est = sketchX.unionSize(sketchY);
  }

  public static List<MetricsResult> computeCorrelationsAfterJoin(
      ColumnPair columnA,
      ColumnPair columnB,
      List<AggregateFunction> functions,
      MetricsResult result) {

    long time0 = System.nanoTime();
    List<NumericJoinAggregation> joins =
        JoinAggregation.numericJoinAggregate(columnA, columnB, functions);
    final long joinTime = System.nanoTime() - time0;

    List<MetricsResult> results = new ArrayList<>(functions.size());

    for (NumericJoinAggregation join : joins) {
      double[] joinedA = join.valuesA;
      double[] joinedB = join.valuesB;

      // correlation is defined only for vectors of length at least two
      if (joinedA.length < minimumIntersection) {
        continue;
      }

      MetricsResult r = result.clone();
      r.aggregate = join.aggregate;
      r.time = new ComputingTime();
      r.time.join = joinTime;

      time0 = System.nanoTime();
      r.corr_rs_actual = SpearmanCorrelation.coefficient(joinedA, joinedB);
      r.time.spearmans = System.nanoTime() - time0;

      time0 = System.nanoTime();
      r.corr_rp_actual = PearsonCorrelation.coefficient(joinedA, joinedB);
      r.time.pearsons = System.nanoTime() - time0;

      time0 = System.nanoTime();
      r.corr_rin_actual = RinCorrelation.coefficient(joinedA, joinedB);
      r.time.rin = System.nanoTime() - time0;

      time0 = System.nanoTime();
      r.corr_rqn_actual = QnCorrelation.correlation(joinedA, joinedB);
      r.time.qn = System.nanoTime() - time0;

      results.add(r);
    }

    return results;
  }
}
