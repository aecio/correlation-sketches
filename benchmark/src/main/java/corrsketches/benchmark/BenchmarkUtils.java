package corrsketches.benchmark;

import com.google.common.collect.ArrayListMultimap;
import corrsketches.CorrelationSketch;
import corrsketches.CorrelationSketch.ImmutableCorrelationSketch;
import corrsketches.CorrelationSketch.ImmutableCorrelationSketch.Paired;
import corrsketches.SketchType;
import corrsketches.benchmark.ComputePairwiseJoinCorrelations.SketchParams;
import corrsketches.benchmark.MetricsResult.Correlations;
import corrsketches.benchmark.PerfResult.ComputingTime;
import corrsketches.benchmark.utils.Sets;
import corrsketches.correlation.BootstrapedPearson;
import corrsketches.correlation.BootstrapedPearson.BootstrapEstimate;
import corrsketches.correlation.Correlation.Estimate;
import corrsketches.correlation.PearsonCorrelation;
import corrsketches.correlation.QnCorrelation;
import corrsketches.correlation.RinCorrelation;
import corrsketches.correlation.SpearmanCorrelation;
import corrsketches.kmv.GKMV;
import corrsketches.kmv.IKMV;
import corrsketches.kmv.KMV;
import corrsketches.statistics.Kurtosis;
import corrsketches.statistics.Stats;
import corrsketches.statistics.Stats.Extent;
import corrsketches.statistics.Variance;
import it.unimi.dsi.fastutil.doubles.DoubleArrayList;
import it.unimi.dsi.fastutil.doubles.DoubleList;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

public class BenchmarkUtils {

  public static final int minimumIntersection = 3; // minimum sample size for correlation is 2

  public static List<PerfResult> computePerformanceStatistics(
      ColumnPair x, ColumnPair y, List<SketchParams> sketchParams) {
    PerfResult result = new PerfResult();

    computeJoinPerformance(x, y, result);

    List<PerfResult> results = new ArrayList<>();
    if (result.interxy_actual >= minimumIntersection) {
      for (SketchParams params : sketchParams) {
        results.add(computeSketchPerformance(result.clone(), x, y, params));
      }
    }

    return results;
  }

  public static PerfResult computeSketchPerformance(
      PerfResult result, ColumnPair x, ColumnPair y, SketchParams sketchParams) {
    // create correlation sketches for the data
    long time0 = System.nanoTime();
    CorrelationSketch sketchX = createCorrelationSketch(x, sketchParams);
    result.build_x_time = System.nanoTime() - time0;

    time0 = System.nanoTime();
    CorrelationSketch sketchY = createCorrelationSketch(y, sketchParams);
    result.build_y_time = System.nanoTime() - time0;

    result.build_time = result.build_x_time + result.build_y_time;

    ImmutableCorrelationSketch iSketchX = createCorrelationSketch(x, sketchParams).toImmutable();
    ImmutableCorrelationSketch iSketchY = createCorrelationSketch(y, sketchParams).toImmutable();

    time0 = System.nanoTime();
    Paired paired = iSketchX.intersection(iSketchY);
    result.sketch_join_time = System.nanoTime() - time0;
    result.sketch_join_size = paired.keys.length;

    if (result.interxy_actual >= minimumIntersection && paired.keys.length >= minimumIntersection) {

      time0 = System.nanoTime();
      PearsonCorrelation.estimate(paired.x, paired.y);
      result.rp_time = System.nanoTime() - time0;

      time0 = System.nanoTime();
      QnCorrelation.estimate(paired.x, paired.y);
      result.rqn_time = System.nanoTime() - time0;

      time0 = System.nanoTime();
      SpearmanCorrelation.estimate(paired.x, paired.y);
      result.rs_time = System.nanoTime() - time0;

      time0 = System.nanoTime();
      RinCorrelation.estimate(paired.x, paired.y);
      result.rrin_time = System.nanoTime() - time0;

      time0 = System.nanoTime();
      BootstrapedPearson.estimate(paired.x, paired.y);
      result.rpm1_time = System.nanoTime() - time0;

      time0 = System.nanoTime();
      BootstrapedPearson.simpleEstimate(paired.x, paired.y);
      result.rpm1s_time = System.nanoTime() - time0;
    }

    result.parameters = sketchParams.toString();
    result.columnId =
        String.format(
            "X(%s,%s,%s) Y(%s,%s,%s)",
            x.keyName, x.columnName, x.datasetId, y.keyName, y.columnName, y.datasetId);

    return result;
  }

  static void computeJoinPerformance(ColumnPair x, ColumnPair y, PerfResult result) {
    HashSet<String> xKeys = new HashSet<>(x.keyValues);
    HashSet<String> yKeys = new HashSet<>(y.keyValues);
    result.cardx_actual = xKeys.size();
    result.cardy_actual = yKeys.size();

    result.interxy_actual = Sets.intersectionSize(xKeys, yKeys);

    // No need compute any statistics when there is not intersection
    if (result.interxy_actual < minimumIntersection) {
      return;
    }

    // correlation ground-truth
    result.time = timedComputePearsonAfterJoin(x, y);
  }

  public static List<MetricsResult> computeStatistics(
      ColumnPair x, ColumnPair y, List<SketchParams> sketchParams) {

    MetricsResult result = new MetricsResult();

    // compute ground-truth statistics
    computeStatisticsGroundTruth(x, y, result);

    List<MetricsResult> results = new ArrayList<>();
    if (result.interxy_actual >= minimumIntersection) {
      for (SketchParams params : sketchParams) {
        results.add(computeSketchStatistics(result.clone(), x, y, params));
      }
    }

    return results;
  }

  private static void computeStatisticsGroundTruth(
      ColumnPair x, ColumnPair y, MetricsResult result) {
    HashSet<String> xKeys = new HashSet<>(x.keyValues);
    HashSet<String> yKeys = new HashSet<>(y.keyValues);
    result.cardx_actual = xKeys.size();
    result.cardy_actual = yKeys.size();

    result.interxy_actual = Sets.intersectionSize(xKeys, yKeys);

    // No need compute any statistics when there is not intersection
    if (result.interxy_actual < minimumIntersection) {
      return;
    }

    result.unionxy_actual = Sets.unionSize(xKeys, yKeys);

    result.jcx_actual = result.interxy_actual / (double) result.cardx_actual;
    result.jcy_actual = result.interxy_actual / (double) result.cardy_actual;

    result.jsxy_actual = result.interxy_actual / (double) result.unionxy_actual;

    result.kurtx_g2_actual = Kurtosis.g2(x.columnValues);
    result.kurty_g2_actual = Kurtosis.g2(y.columnValues);

    final Extent extentX = Stats.extent(x.columnValues);
    result.x_min = extentX.min;
    result.x_max = extentX.max;

    final Extent extentY = Stats.extent(y.columnValues);
    result.y_min = extentY.min;
    result.y_max = extentY.max;

    // correlation ground-truth
    Correlations corrs = computePearsonAfterJoin(x, y);
    result.corr_rp_actual = corrs.pearsons;
    result.corr_rqn_actual = corrs.qn;
    result.corr_rin_actual = corrs.rin;
    result.corr_rs_actual = corrs.spearman;
  }

  public static CorrelationSketch createCorrelationSketch(ColumnPair x, SketchParams sketchParams) {
    IKMV kmv;
    if (sketchParams.type == SketchType.KMV) {
      kmv = KMV.create(x.keyValues, x.columnValues, (int) sketchParams.budget);
    } else {
      kmv = GKMV.create(x.keyValues, x.columnValues, sketchParams.budget);
    }
    return CorrelationSketch.create(kmv);
  }

  public static MetricsResult computeSketchStatistics(
      MetricsResult result, ColumnPair x, ColumnPair y, SketchParams sketchParams) {

    // create correlation sketches for the data
    CorrelationSketch sketchX = createCorrelationSketch(x, sketchParams);
    CorrelationSketch sketchY = createCorrelationSketch(y, sketchParams);

    ImmutableCorrelationSketch iSketchX = createCorrelationSketch(x, sketchParams).toImmutable();
    ImmutableCorrelationSketch iSketchY = createCorrelationSketch(y, sketchParams).toImmutable();
    Paired paired = iSketchX.intersection(iSketchY);

    // Some datasets have large column sizes, but all values can be empty strings (missing data),
    // so we need to check weather the actual cardinality and sketch sizes are large enough.
    if (result.interxy_actual >= minimumIntersection && paired.keys.length >= minimumIntersection) {

      // set operations estimates (jaccard, cardinality, etc)
      computeSetStatisticsEstimates(result, sketchX, sketchY);

      // computes statistics on joined data (e.g., correlations)
      computePairedStatistics(result, paired);
    }

    result.parameters = sketchParams.toString();
    result.columnId =
        String.format(
            "X(%s,%s,%s) Y(%s,%s,%s)",
            x.keyName, x.columnName, x.datasetId, y.keyName, y.columnName, y.datasetId);

    return result;
  }

  private static void computePairedStatistics(MetricsResult result, Paired paired) {

    // Sample size used to estimate correlations
    result.corr_est_sample_size = paired.keys.length;

    // correlation estimates
    Estimate estimate = PearsonCorrelation.estimate(paired.x, paired.y);
    result.corr_rp_est = estimate.coefficient;
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

    Estimate qncorr = QnCorrelation.estimate(paired.x, paired.y);
    result.corr_rqn_est = qncorr.coefficient;
    result.corr_rqn_delta = result.corr_rqn_actual - result.corr_rqn_est;

    Estimate corrSpearman = SpearmanCorrelation.estimate(paired.x, paired.y);
    result.corr_rs_est = corrSpearman.coefficient;
    result.corr_rs_delta = result.corr_rs_actual - result.corr_rs_est;

    Estimate corrRin = RinCorrelation.estimate(paired.x, paired.y);
    result.corr_rin_est = corrRin.coefficient;
    result.corr_rin_delta = result.corr_rin_actual - result.corr_rin_est;

    BootstrapEstimate corrPm1 = BootstrapedPearson.estimate(paired.x, paired.y);
    result.corr_pm1_mean = corrPm1.corrBsMean;
    result.corr_pm1_mean_delta = result.corr_rp_actual - result.corr_pm1_mean;

    result.corr_pm1_median = corrPm1.corrBsMedian;
    result.corr_pm1_median_delta = result.corr_rp_actual - result.corr_pm1_median;

    result.corr_pm1_lb = corrPm1.lowerBound;
    result.corr_pm1_ub = corrPm1.upperBound;

    // Kurtosis of paired variables
    result.kurtx_g2 = Kurtosis.g2(paired.x);
    result.kurtx_G2 = Kurtosis.G2(paired.x);
    result.kurtx_k5 = Kurtosis.k5(paired.x);
    result.kurty_g2 = Kurtosis.G2(paired.y);
    result.kurty_G2 = Kurtosis.G2(paired.y);
    result.kurty_k5 = Kurtosis.k5(paired.y);

    final Extent extentX = Stats.extent(paired.x);
    result.x_min_sample = extentX.min;
    result.x_max_sample = extentX.max;

    final Extent extentY = Stats.extent(paired.y);
    result.y_min_sample = extentY.min;
    result.y_max_sample = extentY.max;

    double[] unitRangeX = Stats.unitRange(paired.x, result.x_min, result.x_max);
    double[] unitRangeY = Stats.unitRange(paired.y, result.y_min, result.y_max);
    result.x_sample_mean = Stats.mean(unitRangeX);
    result.y_sample_mean = Stats.mean(unitRangeY);
    result.x_sample_var = Variance.var(unitRangeX);
    result.y_sample_var = Variance.var(unitRangeY);
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

  public static Correlations computePearsonAfterJoin(ColumnPair query, ColumnPair column) {

    ColumnPair columnA = query;
    ColumnPair columnB = column;

    // create index for primary key in column B
    ArrayListMultimap<String, Double> columnMapB = ArrayListMultimap.create();
    for (int i = 0; i < columnB.keyValues.size(); i++) {
      columnMapB.put(columnB.keyValues.get(i), columnB.columnValues[i]);
    }

    // loop over column B creating to new vectors index for primary key in column B
    DoubleList joinValuesA = new DoubleArrayList();
    DoubleList joinValuesB = new DoubleArrayList();
    for (int i = 0; i < columnA.keyValues.size(); i++) {
      String keyA = columnA.keyValues.get(i);
      double valueA = columnA.columnValues[i];
      List<Double> rowsB = columnMapB.get(keyA);
      if (rowsB != null && !rowsB.isEmpty()) {
        // TODO: We should properly handle cases where 1:N relationships happen.
        // We could could consider the correlation of valueA with an any aggregation function of the
        // list of values from B, e.g. mean, max, sum, count, etc.
        // Currently we are considering only the first seen value, and ignoring everything else,
        // similarly to the correlation sketch implementation.
        joinValuesA.add(valueA);
        joinValuesB.add(rowsB.get(0).doubleValue());
      }
    }

    // correlation is defined only for vectors of length at least two
    Correlations correlations = new Correlations();
    if (joinValuesA.size() < 2) {
      correlations.pearsons = Double.NaN;
      correlations.qn = Double.NaN;
      correlations.spearman = Double.NaN;
      correlations.rin = Double.NaN;
    } else {
      double[] joinedA = joinValuesA.toDoubleArray();
      double[] joinedB = joinValuesB.toDoubleArray();
      correlations.pearsons = PearsonCorrelation.coefficient(joinedA, joinedB);
      correlations.spearman = SpearmanCorrelation.coefficient(joinedA, joinedB);
      correlations.rin = RinCorrelation.coefficient(joinedA, joinedB);
      try {
        correlations.qn = QnCorrelation.correlation(joinedA, joinedB);
      } catch (Exception e) {
        correlations.qn = Double.NaN;
        System.out.printf(
            "Computation of Qn correlation failed for query id=%s [%s]] and column id=%s [%s]. "
                + "Array length after join is %d.\n",
            query.id(), query.toString(), column.id(), column.toString(), joinedA.length);
        System.out.printf("Error stack trace: %s\n", e.toString());
      }
    }

    return correlations;
  }

  public static ComputingTime timedComputePearsonAfterJoin(ColumnPair query, ColumnPair column) {
    ComputingTime time = new ComputingTime();

    ColumnPair columnA = query;
    ColumnPair columnB = column;
    long time0 = System.nanoTime();

    // create index for primary key in column B
    ArrayListMultimap<String, Double> columnMapB = ArrayListMultimap.create();
    for (int i = 0; i < columnB.keyValues.size(); i++) {
      columnMapB.put(columnB.keyValues.get(i), columnB.columnValues[i]);
    }

    // loop over column B creating to new vectors index for primary key in column B
    DoubleList joinValuesA = new DoubleArrayList();
    DoubleList joinValuesB = new DoubleArrayList();
    for (int i = 0; i < columnA.keyValues.size(); i++) {
      String keyA = columnA.keyValues.get(i);
      double valueA = columnA.columnValues[i];
      List<Double> rowsB = columnMapB.get(keyA);
      if (rowsB != null && !rowsB.isEmpty()) {
        // TODO: We should properly handle cases where 1:N relationships happen.
        // We could could consider the correlation of valueA with an any aggregation function of the
        // list of values from B, e.g. mean, max, sum, count, etc.
        // Currently we are considering only the first seen value, and ignoring everything else,
        // similarly to the correlation sketch implementation.
        joinValuesA.add(valueA);
        joinValuesB.add(rowsB.get(0).doubleValue());
      }
    }
    time.join = System.nanoTime() - time0;

    // correlation is defined only for vectors of length at least two
    Correlations correlations = new Correlations();
    if (joinValuesA.size() < 2) {
      correlations.pearsons = Double.NaN;
      correlations.qn = Double.NaN;
      correlations.spearman = Double.NaN;
      correlations.rin = Double.NaN;
    } else {
      double[] joinedA = joinValuesA.toDoubleArray();
      double[] joinedB = joinValuesB.toDoubleArray();

      time0 = System.nanoTime();
      correlations.spearman = SpearmanCorrelation.coefficient(joinedA, joinedB);
      time.spearmans = System.nanoTime() - time0;

      time0 = System.nanoTime();
      correlations.pearsons = PearsonCorrelation.coefficient(joinedA, joinedB);
      time.pearsons = System.nanoTime() - time0;

      time0 = System.nanoTime();
      correlations.rin = RinCorrelation.coefficient(joinedA, joinedB);
      time.rin = System.nanoTime() - time0;

      try {
        time0 = System.nanoTime();
        correlations.qn = QnCorrelation.correlation(joinedA, joinedB);
        time.qn = System.nanoTime() - time0;
      } catch (Exception e) {
        correlations.qn = Double.NaN;
        System.out.printf(
            "Computation of Qn correlation failed for query id=%s [%s]] and column id=%s [%s]. "
                + "Array length after join is %d.\n",
            query.id(), query.toString(), column.id(), column.toString(), joinedA.length);
        System.out.printf("Error stack trace: %s\n", e.toString());
      }
    }

    return time;
  }
}
