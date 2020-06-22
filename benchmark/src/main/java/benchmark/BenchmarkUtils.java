package benchmark;

import benchmark.ComputePairwiseCorrelationJoinsThreads.SketchParams;
import benchmark.Tables.Correlations;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.commons.text.StringEscapeUtils;
import sketches.correlation.Correlation;
import sketches.correlation.CorrelationType;
import sketches.correlation.KMVCorrelationSketch;
import sketches.correlation.KMVCorrelationSketch.CorrelationEstimate;
import sketches.correlation.PearsonCorrelation;
import sketches.correlation.PearsonCorrelation.ConfidenceInterval;
import sketches.correlation.SketchType;
import sketches.kmv.GKMV;
import sketches.kmv.IKMV;
import sketches.kmv.KMV;
import tech.tablesaw.api.CategoricalColumn;
import tech.tablesaw.api.ColumnType;
import tech.tablesaw.api.NumericColumn;
import tech.tablesaw.api.Table;
import tech.tablesaw.io.csv.CsvReadOptions;
import tech.tablesaw.io.csv.CsvReadOptions.Builder;
import utils.Sets;

public class BenchmarkUtils {

  public static final Correlation QN_ESTIMATOR = CorrelationType.get(CorrelationType.ROBUST_QN);
  public static final Correlation RIN_ESTIMATOR = CorrelationType.get(CorrelationType.RIN);
  public static final Correlation SPEARMANS_ESTIMATOR = CorrelationType
      .get(CorrelationType.SPEARMANS);

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
    for (SketchParams params : sketchParams) {
      results.add(computeSketchStatistics(result.clone(), x, y, params));
    }

    return results;
  }

  private static void computeStatisticsGroundTruth(ColumnPair x, ColumnPair y, Result result) {
    HashSet<String> xKeys = new HashSet<>(x.keyValues);
    HashSet<String> yKeys = new HashSet<>(y.keyValues);
    result.cardx_actual = xKeys.size();
    result.cardy_actual = yKeys.size();

    result.interxy_actual = Sets.intersectionSize(xKeys, yKeys);
    result.unionxy_actual = Sets.unionSize(xKeys, yKeys);

    result.jcx_actual = result.interxy_actual / (double) result.cardx_actual;
    result.jcy_actual = result.interxy_actual / (double) result.cardy_actual;

    result.jsxy_actual = result.interxy_actual / (double) result.unionxy_actual;

    // correlation ground-truth
    Correlations corrs = Tables.computePearsonAfterJoin(x, y);
    result.corr_actual = corrs.pearsons;
    result.qncorr_actual = corrs.qn;
    result.corr_rin_actual = corrs.rin;
    result.corr_spearman_actual = corrs.spearman;
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

    //    synchronized (System.out) {
    //      System.out.println();
    //      System.out.printf("x=%s dataset=%s\n", x.columnName, x.datasetId);
    //      System.out.printf("y=%s dataset=%s\n", y.columnName, y.datasetId);
    //      System.out.printf("x.size=%d y.size=%d\n", x.keyValues.size(), y.keyValues.size());
    //      System.out.printf(
    //          "sketch.x.size=%d sketch.y.size=%d\n",
    //          sketchX.getKMinValues().size(), sketchY.getKMinValues().size());
    //    }

    int mininumIntersection = 2; // minimum sample size for correlation is 2
    int mininumSetSize = 2;

    // Some datasets have large column sizes, but all values can be empty strings (missing data),
    // so we need to check weather the actual cardinality and sketch sizes are large enough.

    if (result.interxy_actual >= mininumIntersection
        && result.cardx_actual >= mininumSetSize
        && result.cardy_actual >= mininumSetSize
        && sketchX.getKMinValues().size() >= mininumSetSize
        && sketchY.getKMinValues().size() >= mininumSetSize) {

      // set operations estimates (jaccard, cardinality, etc)
      computeSetStatisticsEstimates(result, sketchX, sketchY);

      // correlation estimates
      CorrelationEstimate estimate = sketchX.correlationTo(sketchY);
      result.corr_est = estimate.coefficient;
      result.corr_est_sample_size = estimate.sampleSize;
      result.corr_delta = result.corr_actual - result.corr_est;

      if (estimate.sampleSize > 2) {
        // statistical significance is only defined for sample size > 2
        int sampleSize = estimate.sampleSize;
        result.corr_est_pvalue2t = PearsonCorrelation.pValueTwoTailed(result.corr_est, sampleSize);

        double alpha = .05;
        result.corr_est_intervals =
            PearsonCorrelation.confidenceInterval(result.corr_est, sampleSize, 1. - alpha);
        result.corr_est_significance =
            PearsonCorrelation.isSignificant(result.corr_est, sampleSize, alpha);
      }

      CorrelationEstimate qncorr = sketchX.correlationTo(sketchY, QN_ESTIMATOR);
      result.qncorr_est = qncorr.coefficient;
      result.qncorr_delta = result.qncorr_actual - result.qncorr_est;

      CorrelationEstimate corrSpearman = sketchX.correlationTo(sketchY, SPEARMANS_ESTIMATOR);
      result.corr_spearman_est = corrSpearman.coefficient;
      result.corr_spearman_delta = result.corr_spearman_actual - result.corr_spearman_est;

      CorrelationEstimate corrRin = sketchX.correlationTo(sketchY, RIN_ESTIMATOR);
      result.corr_rin_est = corrRin.coefficient;
      result.corr_rin_delta = result.corr_rin_actual - result.corr_rin_est;
    }

    result.parameters = sketchParams.toString();
    result.columnId =
        String.format(
            "X(%s,%s,%s) Y(%s,%s,%s)",
            x.keyName, x.columnName, x.datasetId, y.keyName, y.columnName, y.datasetId);

    return result;
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
    // person estimation statistics
    public double corr_actual;
    public double corr_est;
    public double corr_delta;
    public double corr_est_pvalue2t;
    public ConfidenceInterval corr_est_intervals;
    public boolean corr_est_significance;
    // Qn correlation
    public double qncorr_actual;
    public double qncorr_est;
    public double qncorr_delta;
    // Spearman correlation
    public double corr_spearman_est;
    public double corr_spearman_actual;
    public double corr_spearman_delta;
    // RIN correlation
    public double corr_rin_est;
    public double corr_rin_actual;
    public double corr_rin_delta;
    // others
    public String parameters;
    public String columnId;
    public int corr_est_sample_size;


    public static String header() {
      return String.format(
          "Contain  Pearson  Estimation  Error  CardQ  CardC  p-value  Interval           Sig    Name");
    }

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
              // correlations
              + "corr_est,"
              + "corr_actual,"
              + "corr_delta,"
              + "corr_est_sample_size,"
              + "corr_est_pvalue2t,"
              + "corr_est_intervals,"
              + "corr_est_significance,"
              // Qn correlations
              + "qncorr_est,"
              + "qncorr_actual,"
              + "qncorr_delta,"
              // Spearman correlations
              + "corr_spearman_est,"
              + "corr_spearman_actual,"
              + "corr_spearman_delta,"
              // RIN correlations
              + "corr_rin_est,"
              + "corr_rin_actual,"
              + "corr_rin_delta,"
              // others
              + "parameters,"
              + "column");
    }

    public String csvLine() {
      return String.format(
          ""
              + "%.3f,%.3f,%.3f,%.3f,%.3f,%.3f,"
              + "%.2f,%d,%.2f,%d,"
              + "%.2f,%d,%.2f,%d,"
              + "%.3f,%.3f,%.3f,%d,%.3f,%s,%s,"
              + "%.3f,%.3f,%.3f," // Qn
              + "%.3f,%.3f,%.3f," // Spearman
              + "%.3f,%.3f,%.3f," // RIN
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
          // correlations
          corr_est,
          corr_actual,
          corr_delta,
          corr_est_sample_size,
          corr_est_pvalue2t,
          StringEscapeUtils.escapeCsv(String.valueOf(corr_est_intervals)),
          StringEscapeUtils.escapeCsv(String.valueOf(corr_est_significance)),
          // Qn correlations
          qncorr_est,
          qncorr_actual,
          qncorr_delta,
          // Spearman correlations
          corr_spearman_est,
          corr_spearman_actual,
          corr_spearman_delta,
          // RIN correlations
          corr_rin_est,
          corr_rin_actual,
          corr_rin_delta,
          // others
          StringEscapeUtils.escapeCsv(parameters),
          StringEscapeUtils.escapeCsv(columnId));
    }

    @Override
    public String toString() {
      return String.format(
          "%+.4f  %+.4f  %+.7f  %+.2f  %.2f   %.2f   %.3f    %-17s  %-5s  %s\n",
          jcx_est,
          corr_actual,
          corr_est,
          corr_delta,
          cardx_est,
          cardy_est,
          corr_est_pvalue2t,
          corr_est_intervals,
          corr_est_significance,
          columnId);
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
