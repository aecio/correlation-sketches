package benchmark;

import benchmark.ComputePairwiseCorrelationJoinsThreads.SketchParams;
import benchmark.Tables.Correlations;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.commons.text.StringEscapeUtils;
import sketches.correlation.Correlation.Estimate;
import sketches.correlation.KMVCorrelationSketch;
import sketches.correlation.KMVCorrelationSketch.ImmutableCorrelationSketch;
import sketches.correlation.KMVCorrelationSketch.ImmutableCorrelationSketch.Paired;
import sketches.correlation.PearsonCorrelation;
import sketches.correlation.Qn;
import sketches.correlation.SketchType;
import sketches.correlation.estimators.BootstrapedPearson;
import sketches.correlation.estimators.BootstrapedPearson.BootstrapEstimate;
import sketches.correlation.estimators.RinCorrelation;
import sketches.correlation.estimators.SpearmanCorrelation;
import sketches.kmv.GKMV;
import sketches.kmv.IKMV;
import sketches.kmv.KMV;
import sketches.statistics.Kurtosis;
import sketches.statistics.Stats;
import sketches.statistics.Stats.Extent;
import tech.tablesaw.api.CategoricalColumn;
import tech.tablesaw.api.ColumnType;
import tech.tablesaw.api.NumericColumn;
import tech.tablesaw.api.Table;
import tech.tablesaw.io.csv.CsvReadOptions;
import tech.tablesaw.io.csv.CsvReadOptions.Builder;
import utils.Sets;

public class BenchmarkUtils {

  public static final int minimumIntersection = 3; // minimum sample size for correlation is 2

  public static Set<ColumnPair> readAllColumnPairs(List<String> allFiles, int minRows) {
    Set<ColumnPair> allPairs = new HashSet<>();
    for (int i = 0; i < allFiles.size(); i++) {
      String dataset = allFiles.get(i);
      try {
        Iterator<ColumnPair> it = readColumnPairs(dataset, minRows);
        while (it.hasNext()) {
          allPairs.add(it.next());
        }
      } catch (Exception e) {
        System.err.println("Failed to read dataset: " + dataset);
        System.err.println(e.toString());
      }
    }
    return allPairs;
  }

  public static Iterator<ColumnPair> readColumnPairs(String datasetFilePath, int minRows) {
    try {
      Table table = readTable(CsvReadOptions.builderFromFile(datasetFilePath));
      return readColumnPairs(Paths.get(datasetFilePath).getFileName().toString(), table, minRows);
    } catch (Exception e) {
      System.out.println("\nFailed to read dataset from file: " + datasetFilePath);
      e.printStackTrace(System.out);
      return Collections.emptyIterator();
    }
  }

  public static Iterator<ColumnPair> readColumnPairs(
      String datasetName, InputStream is, int minRows) {
    try {
      Table table = readTable(CsvReadOptions.builder(is));
      return readColumnPairs(datasetName, table, minRows);
    } catch (Exception e) {
      System.out.println("\nFailed to read dataset from input stream: " + datasetName);
      e.printStackTrace(System.out);
      return Collections.emptyIterator();
    }
  }

  public static Iterator<ColumnPair> readColumnPairs(String datasetName, Table df, int minRows) {
    System.out.println("\nDataset: " + datasetName);

    System.out.printf("Row count: %d \n", df.rowCount());

    List<CategoricalColumn<String>> categoricalColumns = getStringColumns(df);
    System.out.println("Categorical columns: " + categoricalColumns.size());

    List<NumericColumn<?>> numericColumns = df.numericColumns();
    System.out.println("Numeric columns: " + numericColumns.size());

    if (df.rowCount() < minRows) {
      System.out.println("Column pairs: 0");
      return Collections.emptyIterator();
    }

    // Create a list of all column pairs
    List<ColumnEntry> pairs = new ArrayList<>();
    for (CategoricalColumn<?> key : categoricalColumns) {
      for (NumericColumn<?> column : numericColumns) {
        pairs.add(new ColumnEntry(key, column));
      }
    }
    System.out.println("Column pairs: " + pairs.size());
    if (pairs.isEmpty()) {
      return Collections.emptyIterator();
    }

    // Create a "lazy" iterator that creates one ColumnPair at a time to avoid overloading memory
    // with many large column pairs.
    Iterator<ColumnEntry> it = pairs.iterator();
    return new Iterator<ColumnPair>() {
      ColumnEntry nextPair = it.next();

      @Override
      public boolean hasNext() {
        return nextPair != null;
      }

      @Override
      public ColumnPair next() {
        ColumnEntry tmp = nextPair;
        nextPair = it.hasNext() ? it.next() : null;
        return Tables.createColumnPair(datasetName, tmp.key, tmp.column);
      }
    };
  }

  static class ColumnEntry {

    CategoricalColumn<?> key;
    NumericColumn<?> column;

    ColumnEntry(CategoricalColumn<?> key, NumericColumn<?> column) {
      this.key = key;
      this.column = column;
    }
  }

  private static Table readTable(Builder csvReadOptionsBuilder) throws IOException {
    Table table =
        Table.read()
            .csv(
                csvReadOptionsBuilder
                    .sample(true)
                    .sampleSize(5_000_000)
                    .maxCharsPerColumn(10_000)
                    .missingValueIndicator("-"));
    return table;
  }

  public static List<Set<String>> readAllKeyColumns(String dataset) throws IOException {
    Table df = readTable(CsvReadOptions.builderFromFile(dataset));
    List<CategoricalColumn<String>> categoricalColumns = getStringColumns(df);
    List<Set<String>> allColumns = new ArrayList<>();
    for (CategoricalColumn<String> column : categoricalColumns) {
      Set<String> keySet = new HashSet<>();
      Iterator<String> it = column.iterator();
      while (it.hasNext()) {
        keySet.add(it.next());
      }
      allColumns.add(keySet);
    }
    return allColumns;
  }

  private static List<CategoricalColumn<String>> getStringColumns(Table df) {
    return df.columns()
        .stream()
        .filter(e -> e.type() == ColumnType.STRING || e.type() == ColumnType.TEXT)
        .map(e -> (CategoricalColumn<String>) e)
        .collect(Collectors.toList());
  }

  public static List<String> findAllCSVs(String basePath) throws IOException {

    List<String> allFiles =
        Files.walk(Paths.get(basePath))
            .filter(p -> p.toString().endsWith(".csv"))
            .filter(Files::isRegularFile)
            .map(p -> p.toString())
            .collect(Collectors.toList());

    return allFiles;
  }

  public static List<Result> computeStatistics(
      ColumnPair x, ColumnPair y, List<SketchParams> sketchParams) {

    Result result = new Result();

    // compute ground-truth statistics
    computeStatisticsGroundTruth(x, y, result);

    List<Result> results = new ArrayList<>();
    if (result.interxy_actual >= minimumIntersection) {
      for (SketchParams params : sketchParams) {
        results.add(computeSketchStatistics(result.clone(), x, y, params));
      }
    }

    return results;
  }

  private static void computeStatisticsGroundTruth(ColumnPair x, ColumnPair y, Result result) {
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
    Correlations corrs = Tables.computePearsonAfterJoin(x, y);
    result.corr_rp_actual = corrs.pearsons;
    result.corr_rqn_actual = corrs.qn;
    result.corr_rin_actual = corrs.rin;
    result.corr_rs_actual = corrs.spearman;
  }

  public static KMVCorrelationSketch createCorrelationSketch(
      ColumnPair x, SketchParams sketchParams) {
    IKMV kmv;
    if (sketchParams.type == SketchType.KMV) {
      kmv = KMV.create(x.keyValues, x.columnValues, (int) sketchParams.budget);
    } else {
      kmv = GKMV.create(x.keyValues, x.columnValues, sketchParams.budget);
    }
    return KMVCorrelationSketch.create(kmv);
  }

  public static Result computeSketchStatistics(
      Result result, ColumnPair x, ColumnPair y, SketchParams sketchParams) {

    // create correlation sketches for the data
    KMVCorrelationSketch sketchX = createCorrelationSketch(x, sketchParams);
    KMVCorrelationSketch sketchY = createCorrelationSketch(y, sketchParams);

    ImmutableCorrelationSketch iSketchX = createCorrelationSketch(x, sketchParams).toImmutable();
    ImmutableCorrelationSketch iSketchY = createCorrelationSketch(y, sketchParams).toImmutable();
    Paired paired = iSketchX.intersection(iSketchY);

    // Some datasets have large column sizes, but all values can be empty strings (missing data),
    // so we need to check weather the actual cardinality and sketch sizes are large enough.
    if (result.interxy_actual >= minimumIntersection
        && paired.keys.length >= minimumIntersection) {

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

  private static void computePairedStatistics(Result result, Paired paired) {

    // Sample size used to estimate correlations
    result.corr_est_sample_size = paired.keys.length;

    // correlation estimates
    Estimate estimate = PearsonCorrelation.estimate(paired.x, paired.y);
    result.corr_rp_est = estimate.coefficient;
    result.corr_rp_delta = result.corr_rp_actual - result.corr_rp_est;

//    if (estimate.sampleSize > 2) {
//      // statistical significance is only defined for sample size > 2
//      int sampleSize = estimate.sampleSize;
//      result.corr_rp_est_pvalue2t = PearsonCorrelation.pValueTwoTailed(result.corr_rp_est, sampleSize);
//
//      double alpha = .05;
//      result.corr_rp_est_fisher =
//          PearsonCorrelation.confidenceInterval(result.corr_rp_est, sampleSize, 1. - alpha);
//      result.corr_est_significance =
//          PearsonCorrelation.isSignificant(result.corr_rp_est, sampleSize, alpha);
//    }

    Estimate qncorr = Qn.estimate(paired.x, paired.y);
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

  }

  private static void computeSetStatisticsEstimates(
      Result result, KMVCorrelationSketch sketchX, KMVCorrelationSketch sketchY) {
    result.jcx_est = sketchX.containment(sketchY);
    result.jcy_est = sketchY.containment(sketchX);
    result.jsxy_est = sketchX.jaccard(sketchY);
    result.cardx_est = sketchX.cardinality();
    result.cardy_est = sketchY.cardinality();
    result.interxy_est = sketchX.intersectionSize(sketchY);
    result.unionxy_est = sketchX.unionSize(sketchY);
  }

  public static class Result implements Cloneable {

    // kmv estimation statistics
    public double jcx_est;
    public double jcy_est;
    public double jsxy_est;
    public double jcx_actual;
    public double jcy_actual;
    public double jsxy_actual;
    // cardinality statistics
    public double cardx_est;
    public double cardy_est;
    public int cardx_actual;
    public int cardy_actual;
    // set statistics
    public double interxy_est;
    public double unionxy_est;
    public int interxy_actual;
    public int unionxy_actual;
    // Correlation sample size
    public int corr_est_sample_size;
    // Person's correlation
    public double corr_rp_actual;
    public double corr_rp_est;
    public double corr_rp_delta;
    // Pearson's Fisher CI
//    public double corr_rp_est_pvalue2t;
//    public ConfidenceInterval corr_rp_est_fisher;
//    public boolean corr_est_significance;
    // Qn correlation
    public double corr_rqn_actual;
    public double corr_rqn_est;
    public double corr_rqn_delta;
    // Spearman correlation
    public double corr_rs_est;
    public double corr_rs_actual;
    public double corr_rs_delta;
    // RIN correlation
    public double corr_rin_est;
    public double corr_rin_actual;
    public double corr_rin_delta;
    // PM1 bootstrap
    public double corr_pm1_mean;
    public double corr_pm1_mean_delta;
    public double corr_pm1_median;
    public double corr_pm1_median_delta;
    public double corr_pm1_lb;
    public double corr_pm1_ub;
    // Kurtosis
    public double kurtx_g2_actual;
    public double kurty_g2_actual;
    public double kurtx_g2;
    public double kurtx_G2;
    public double kurtx_k5;
    public double kurty_g2;
    public double kurty_G2;
    public double kurty_k5;
    // Variable sample extents
    public double y_min_sample;
    public double y_max_sample;
    public double x_min_sample;
    public double x_max_sample;
    // Variable extents
    public double x_min;
    public double x_max;
    public double y_min;
    public double y_max;
    // others
    public String parameters;
    public String columnId;


    public static String csvHeader() {
      return String.format(
          ""
              // jaccard
              + "jcx_est,"
              + "jcy_est,"
              + "jcx_actual,"
              + "jcy_actual,"
              + "jsxy_est,"
              + "jsxy_actual,"
              // cardinalities
              + "cardx_est,"
              + "cardx_actual,"
              + "cardy_est,"
              + "cardy_actual,"
              // set statistics
              + "interxy_est,"
              + "interxy_actual,"
              + "unionxy_est,"
              + "unionxy_actual,"
              // Correlation sample size
              + "corr_est_sample_size,"
              // Pearson's correlations
              + "corr_rp_est,"
              + "corr_rp_actual,"
              + "corr_rp_delta,"
              // Pearson's Fisher CI
//              + "corr_rp_est_pvalue2t,"
//              + "corr_rp_est_fisher_ub,"
//              + "corr_rp_est_fisher_lb,"
              // Qn correlations
              + "corr_rqn_est,"
              + "corr_rqn_actual,"
              + "corr_rqn_delta,"
              // Spearman correlations
              + "corr_rs_est,"
              + "corr_rs_actual,"
              + "corr_rs_delta,"
              // RIN correlations
              + "corr_rin_est,"
              + "corr_rin_actual,"
              + "corr_rin_delta,"
              // PM1 bootstrap
              + "corr_pm1_mean,"
              + "corr_pm1_mean_delta,"
              + "corr_pm1_median,"
              + "corr_pm1_median_delta,"
              + "corr_pm1_lb,"
              + "corr_pm1_ub,"
              // Kurtosis
              + "kurtx_g2_actual,"
              + "kurtx_g2,"
              + "kurtx_G2,"
              + "kurtx_k5,"
              + "kurty_g2_actual,"
              + "kurty_g2,"
              + "kurty_G2,"
              + "kurty_k5,"
              // Variable sample extents
              + "y_min_sample,"
              + "y_max_sample,"
              + "x_min_sample,"
              + "x_max_sample,"
              // Variable extents
              + "x_min,"
              + "x_max,"
              + "y_min,"
              + "y_max,"
              // others
              + "parameters,"
              + "column");
    }

    public String csvLine() {
      return String.format(
          ""
              + "%.3f,%.3f,%.3f,%.3f,%.3f,%.3f," // Jaccard
              + "%.2f,%d,%.2f,%d," // cardinalities
              + "%.2f,%d,%.2f,%d," // set statistics
              + "%d," // sample size
              + "%.3f,%.3f,%.3f," // Pearson's
//              + "%.3f,%.3f,%.3f," // Pearson's Fisher CI
              + "%.3f,%.3f,%.3f," // Qn
              + "%.3f,%.3f,%.3f," // Spearman's
              + "%.3f,%.3f,%.3f," // RIN
              + "%.3f,%.3f,%.3f,%.3f,%.3f,%.3f," // PM1
              + "%.3f,%.3f,%.3f,%.3f,%.3f,%.3f,%.3f,%.3f," // Kurtosis
              + "%f,%f,%f,%f," // Variable sample extents
              + "%f,%f,%f,%f," // Variable extents
              + "%s,%s",
          // jaccard
          jcx_est,
          jcy_est,
          jcx_actual,
          jcy_actual,
          jsxy_est,
          jsxy_actual,
          // cardinalities
          cardx_est,
          cardx_actual,
          cardy_est,
          cardy_actual,
          // set statistics
          interxy_est,
          interxy_actual,
          unionxy_est,
          unionxy_actual,
          // sample
          corr_est_sample_size,
          // Pearson's correlations
          corr_rp_est,
          corr_rp_actual,
          corr_rp_delta,
          // Pearson's Fisher CI
//          corr_rp_est_pvalue2t,
//          corr_rp_est_fisher.lowerBound,
//          corr_rp_est_fisher.upperBound,
          // Qn correlations
          corr_rqn_est,
          corr_rqn_actual,
          corr_rqn_delta,
          // Spearman's correlations
          corr_rs_est,
          corr_rs_actual,
          corr_rs_delta,
          // RIN correlations
          corr_rin_est,
          corr_rin_actual,
          corr_rin_delta,
          // PM1 bootstrap
          corr_pm1_mean,
          corr_pm1_mean_delta,
          corr_pm1_median,
          corr_pm1_median_delta,
          corr_pm1_lb,
          corr_pm1_ub,
          // Kurtosis
          kurtx_g2_actual,
          kurtx_g2,
          kurtx_G2,
          kurtx_k5,
          kurty_g2_actual,
          kurty_g2,
          kurty_G2,
          kurty_k5,
          // Variable sample extents
          y_min_sample,
          y_max_sample,
          x_min_sample,
          x_max_sample,
          // Variable extents
          x_min,
          x_max,
          y_min,
          y_max,
          // others
          StringEscapeUtils.escapeCsv(parameters),
          StringEscapeUtils.escapeCsv(columnId));
    }

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
