package corrsketches.benchmark;

import corrsketches.CorrelationSketch;
import corrsketches.CorrelationSketch.ImmutableCorrelationSketch;
import corrsketches.CorrelationSketch.ImmutableCorrelationSketch.Join;
import corrsketches.aggregations.AggregateFunction;
import corrsketches.benchmark.ComputePairwiseJoinCorrelations.SketchParams;
import corrsketches.benchmark.utils.Sets;
import corrsketches.correlation.BootstrapedPearson;
import corrsketches.correlation.PearsonCorrelation;
import corrsketches.correlation.QnCorrelation;
import corrsketches.correlation.RinCorrelation;
import corrsketches.correlation.SpearmanCorrelation;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;

public class CorrelationPerformanceBenchmark implements Benchmark {

  @Override
  public List<String> run(
      ColumnPair x,
      ColumnPair y,
      List<SketchParams> sketchParams,
      List<AggregateFunction> functions) {
    return measurePerformance(x, y, sketchParams, functions);
  }

  @Override
  public String csvHeader() {
    return PerfResult.csvHeader();
  }

  public static List<String> measurePerformance(
      ColumnPair x,
      ColumnPair y,
      List<SketchParams> sketchParams,
      List<AggregateFunction> functions) {

    List<PerfResult> fullJoinResults = measurePerformanceOnFullJoin(x, y, functions);

    List<String> results = new ArrayList<>();
    for (PerfResult result : fullJoinResults) {
      // we don't need to report column pairs that have no intersection at all
      if (Double.isFinite(result.interxy_actual) && result.interxy_actual >= 2) {
        for (SketchParams params : sketchParams) {
          var r = measureSketchPerformance(result.clone(), x, y, params, result.aggregate);
          results.add(r.csvLine() + "\n");
        }
      }
    }

    return results;
  }

  public static PerfResult measureSketchPerformance(
      PerfResult result,
      ColumnPair x,
      ColumnPair y,
      SketchParams sketchParams,
      AggregateFunction function) {

    // create correlation sketches for the data
    long time0 = System.nanoTime();
    CorrelationSketch sketchX =
        CorrelationStatsBenchmark.createCorrelationSketch(x, sketchParams, function);
    result.build_x_time = System.nanoTime() - time0;

    time0 = System.nanoTime();
    CorrelationSketch sketchY =
        CorrelationStatsBenchmark.createCorrelationSketch(y, sketchParams, function);
    result.build_y_time = System.nanoTime() - time0;

    result.build_time = result.build_x_time + result.build_y_time;

    ImmutableCorrelationSketch iSketchX = sketchX.toImmutable();
    ImmutableCorrelationSketch iSketchY = sketchY.toImmutable();

    time0 = System.nanoTime();
    Join join = iSketchX.join(iSketchY);
    result.sketch_join_time = System.nanoTime() - time0;
    result.sketch_join_size = join.keys.length;

    if (result.interxy_actual >= CorrelationStatsBenchmark.minimumIntersection
        && join.keys.length >= CorrelationStatsBenchmark.minimumIntersection) {

      time0 = System.nanoTime();
      PearsonCorrelation.estimate(join.x, join.y);
      result.rp_time = System.nanoTime() - time0;

      time0 = System.nanoTime();
      QnCorrelation.estimate(join.x, join.y);
      result.rqn_time = System.nanoTime() - time0;

      time0 = System.nanoTime();
      SpearmanCorrelation.estimate(join.x, join.y);
      result.rs_time = System.nanoTime() - time0;

      time0 = System.nanoTime();
      RinCorrelation.estimate(join.x, join.y);
      result.rrin_time = System.nanoTime() - time0;

      time0 = System.nanoTime();
      BootstrapedPearson.estimate(join.x, join.y);
      result.rpm1_time = System.nanoTime() - time0;

      time0 = System.nanoTime();
      BootstrapedPearson.simpleEstimate(join.x, join.y);
      result.rpm1s_time = System.nanoTime() - time0;
    }

    result.parameters = sketchParams.toString();
    result.columnId =
        String.format(
            "X(%s,%s,%s) Y(%s,%s,%s)",
            x.keyName, x.columnName, x.datasetId, y.keyName, y.columnName, y.datasetId);

    return result;
  }

  static List<PerfResult> measurePerformanceOnFullJoin(
      ColumnPair x, ColumnPair y, List<AggregateFunction> functions) {

    PerfResult result = new PerfResult();

    HashSet<String> xKeys = new HashSet<>(x.keyValues);
    HashSet<String> yKeys = new HashSet<>(y.keyValues);
    result.cardx_actual = xKeys.size();
    result.cardy_actual = yKeys.size();
    result.interxy_actual = Sets.intersectionSize(xKeys, yKeys);

    // No need compute any statistics when there is no intersection
    if (result.interxy_actual < CorrelationStatsBenchmark.minimumIntersection) {
      return Collections.emptyList();
    }

    // correlation ground-truth
    List<MetricsResult> groundTruthResults =
        CorrelationStatsBenchmark.computeCorrelationsAfterJoin(
            x, y, functions, new MetricsResult());

    List<PerfResult> perfResults = new ArrayList<>();
    for (MetricsResult r : groundTruthResults) {
      PerfResult p = result.clone();
      p.time = r.time;
      p.aggregate = r.aggregate;
      perfResults.add(p);
    }

    return perfResults;
  }
}
