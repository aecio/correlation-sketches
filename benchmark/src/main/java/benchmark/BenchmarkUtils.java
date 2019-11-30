package benchmark;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.commons.text.StringEscapeUtils;
import sketches.correlation.KMVCorrelationSketch;
import sketches.correlation.KMVCorrelationSketch.CorrelationEstimate;
import sketches.correlation.PearsonCorrelation;
import sketches.correlation.PearsonCorrelation.ConfidenceInterval;
import sketches.correlation.Sketches;
import sketches.correlation.Sketches.Type;
import sketches.kmv.GKMV;
import sketches.kmv.KMV;
import tech.tablesaw.api.CategoricalColumn;
import tech.tablesaw.api.ColumnType;
import tech.tablesaw.api.NumericColumn;
import tech.tablesaw.api.Table;
import tech.tablesaw.io.csv.CsvReadOptions;
import utils.Sets;

public class BenchmarkUtils {

  public static Set<ColumnPair> readAllColumnPairs(List<String> allFiles, int minRows) {
    Set<ColumnPair> allPairs = new HashSet<>();
    for (int i = 0; i < allFiles.size(); i++) {
      String dataset = allFiles.get(i);
      try {
        allPairs.addAll(readColumnPairs(dataset, minRows));
      } catch (Exception e) {
        System.err.println("Failed to read dataset: " + dataset);
        System.err.println(e.toString());
      }
    }
    return allPairs;
  }

  public static Set<ColumnPair> readColumnPairs(String dataset, int minRows) throws IOException {
    InputStream fileInputStream = new FileInputStream(dataset);
    return readColumnPairs(dataset, fileInputStream, minRows);
  }

  public static Set<ColumnPair> readColumnPairs(
      String datasetName, InputStream fileInputStream, int minRows) throws IOException {
    System.out.println("\nDataset: " + datasetName);
    Table df =
        Table.read()
            .csv(
                CsvReadOptions.builder(fileInputStream)
                    .maxCharsPerColumn(10000)
                    .missingValueIndicator("-"));

    System.out.printf("Row count: %d \n", df.rowCount());

    List<CategoricalColumn<String>> categoricalColumns = getStringColumns(df);
    System.out.println("Categorical columns: " + categoricalColumns.size());

    List<NumericColumn<?>> numericColumns = df.numericColumns();
    System.out.println("Numeric columns: " + numericColumns.size());

    Set<ColumnPair> pairs = new HashSet<>();
    if (df.rowCount() < minRows) {
      System.out.println("Column pairs: " + pairs.size());
      return pairs;
    }
    for (CategoricalColumn<?> key : categoricalColumns) {
      for (NumericColumn<?> column : numericColumns) {
        try {
          pairs.add(Tables.createColumnPair(datasetName, key, column));
        } catch (Exception e) {
          e.printStackTrace();
          continue;
        }
      }
    }
    System.out.println("Column pairs: " + pairs.size());
    return pairs;
  }

  public static List<Set<String>> readAllKeyColumns(String dataset) throws IOException {
    InputStream fileInputStream = new FileInputStream(dataset);
    Table df =
        Table.read()
            .csv(
                CsvReadOptions.builder(fileInputStream)
                    .maxCharsPerColumn(10000)
                    .missingValueIndicator("-"));
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
    return df.columns().stream()
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

  public static Result computeStatistics(
      ColumnPair x, ColumnPair y, Sketches.Type type, double nhf) {

    Result result = new Result();

    // compute ground-truth statistics
    computeSetStatisticsGroundTruth(x, y, result);

    // create correlation sketches for the data
    KMVCorrelationSketch sketchX;
    KMVCorrelationSketch sketchY;
    if (type == Type.KMV) {
      int k = (int) nhf;
      result.parameters = "KMV(k=" + k + ")";
      KMV kmvX = KMV.create(x.keyValues, x.columnValues, k);
      KMV kmvY = KMV.create(y.keyValues, y.columnValues, k);
      sketchX = new KMVCorrelationSketch(kmvX);
      sketchY = new KMVCorrelationSketch(kmvY);
    } else {
      double t = nhf;
      result.parameters = "GKMV(t=" + t + ")";
      GKMV gkmvX = GKMV.create(x.keyValues, x.columnValues, t);
      GKMV gkmvY = GKMV.create(y.keyValues, y.columnValues, t);
      sketchX = new KMVCorrelationSketch(gkmvX);
      sketchY = new KMVCorrelationSketch(gkmvY);
    }

//    synchronized (System.out) {
//      System.out.println();
//      System.out.printf("x=%s dataset=%s\n", x.columnName, x.datasetId);
//      System.out.printf("y=%s dataset=%s\n", y.columnName, y.datasetId);
//      System.out.printf("x.size=%d y.size=%d\n", x.keyValues.size(), y.keyValues.size());
//      System.out.printf(
//          "sketch.x.size=%d sketch.y.size=%d\n",
//          sketchX.getKMinValues().size(), sketchY.getKMinValues().size());
//    }

    int mininumIntersection = 3; // minimum sample size for correlation is 3
    int mininumSetSize = 3;

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

      if (estimate.sampleSize > mininumIntersection) {
        result.corr_est = estimate.coefficient;
        result.corr_est_sample_size = estimate.sampleSize;
        result.corr_delta = result.corr_actual - result.corr_est;

        int sampleSize = estimate.sampleSize;
        result.corr_est_pvalue2t = PearsonCorrelation.pValueTwoTailed(result.corr_est, sampleSize);

        double alpha = .05;
        result.corr_est_intervals =
                PearsonCorrelation.confidenceInterval(result.corr_est, sampleSize, 1. - alpha);
        result.corr_est_significance =
                PearsonCorrelation.isSignificant(result.corr_est, sampleSize, alpha);

        // correlation ground-truth
        result.corr_actual = Tables.computePearsonAfterJoin(x, y);
      }

    }

    result.columnId =
        String.format(
            "X(%s,%s,%s) Y(%s,%s,%s)",
            x.keyName, x.columnName, x.datasetId, y.keyName, y.columnName, y.datasetId);

    return result;
  }

  private static void computeSetStatisticsGroundTruth(ColumnPair x, ColumnPair y, Result result) {
    HashSet<String> xKeys = new HashSet<>(x.keyValues);
    HashSet<String> yKeys = new HashSet<>(y.keyValues);
    result.cardx_actual = xKeys.size();
    result.cardy_actual = yKeys.size();

    result.interxy_actual = Sets.intersectionSize(xKeys, yKeys);
    result.unionxy_actual = Sets.unionSize(xKeys, yKeys);

    result.jcx_actual = result.interxy_actual / (double) result.cardx_actual;
    result.jcy_actual = result.interxy_actual / (double) result.cardy_actual;

    result.jsxy_actual = result.interxy_actual / (double) result.unionxy_actual;
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

  public static class Result {

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
  }
}
