package benchmark;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.commons.text.StringEscapeUtils;
import sketches.correlation.KMVCorrelationSketch;
import sketches.correlation.PearsonCorrelation;
import sketches.correlation.PearsonCorrelation.ConfidenceInterval;
import tech.tablesaw.api.CategoricalColumn;
import tech.tablesaw.api.NumericColumn;
import tech.tablesaw.api.StringColumn;
import tech.tablesaw.api.Table;
import tech.tablesaw.io.csv.CsvReadOptions;

public class BenchmarkUtils {

  public static Set<ColumnPair> readAllColumnPairs(List<String> allFiles) {
    Set<ColumnPair> allPairs = new HashSet<>();
    for (int i = 0; i < allFiles.size(); i++) {
      String dataset = allFiles.get(i);
      try {
        allPairs.addAll(readColumnPairs(dataset));
      } catch (Exception e) {
        System.err.println("Failed to read dataset: " + dataset);
        System.err.println(e.toString());
      }
    }
    return allPairs;
  }

  public static Set<ColumnPair> readColumnPairs(String dataset) throws IOException {
    InputStream fileInputStream = new FileInputStream(dataset);
    return readColumnPairs(dataset, fileInputStream);
  }

  public static Set<ColumnPair> readColumnPairs(String datasetName, InputStream fileInputStream)
      throws IOException {

    Table df =
        Table.read()
            .csv(
                CsvReadOptions.builder(fileInputStream)
                    .maxCharsPerColumn(10000)
                    .missingValueIndicator("-"));

    System.out.printf("Row count: %d  File: %s\n", df.rowCount(), datasetName);

    List<StringColumn> categoricalColumns = Arrays.asList(df.stringColumns());
    System.out.println("Categorical columns: " + categoricalColumns.size());

    List<NumericColumn<?>> numericColumns = df.numericColumns();
    System.out.println("Numeric columns: " + numericColumns.size());

    Set<ColumnPair> pairs = new HashSet<>();
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
    return pairs;
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

  public static Result computeStatistics(int nhf, ColumnPair query, ColumnPair column) {

    KMVCorrelationSketch querySketch =
        new KMVCorrelationSketch(query.keyValues, query.columnValues, nhf);

    KMVCorrelationSketch columnSketch =
        new KMVCorrelationSketch(column.keyValues, column.columnValues, nhf);

    Result result = new Result();

    result.estimatedCorrelation = querySketch.correlationTo(columnSketch);
    if (Double.isNaN(result.estimatedCorrelation)) {
      return null;
    }

    result.actualCorrelation = Tables.computePearsonAfterJoin(query, column);
    if (Double.isNaN(result.actualCorrelation)) {
      return null;
    }

    result.columnId =
        String.format(
            "Q(%s,%s,%s) C(%s %s %s)",
            query.keyName,
            query.columnName,
            query.datasetId,
            column.keyName,
            column.columnName,
            column.datasetId);
    int actualCardinalityQ = new HashSet<>(query.keyValues).size();
    int actualCardinalityC = new HashSet<>(column.keyValues).size();
    BenchmarkUtils.computeStatistics(
        nhf, result, querySketch, columnSketch, actualCardinalityQ, actualCardinalityC);
    return result;
  }

  public static void computeStatistics(
      int nhf,
      Result result,
      KMVCorrelationSketch querySketch,
      KMVCorrelationSketch columnSketch,
      int actualCardinalityQ,
      int actualCardinalityC) {
    result.deltaCorrelation = result.actualCorrelation - result.estimatedCorrelation;
    result.jcx = querySketch.containment(columnSketch);
    result.jcy = columnSketch.containment(querySketch);
    result.jsx = querySketch.jaccard(columnSketch);
    result.jsy = querySketch.containment(columnSketch);
    result.cardinalityQ = querySketch.cardinality();
    result.cardinalityC = columnSketch.cardinality();
    result.actualCardinalityQ = actualCardinalityQ;
    result.actualCardinalityC = actualCardinalityC;
    result.cardinalityC = columnSketch.cardinality();
    result.pValueTwoTailed = PearsonCorrelation.pValueTwoTailed(result.estimatedCorrelation, nhf);
    result.confidenceInterval =
        PearsonCorrelation.confidenceInterval(result.estimatedCorrelation, nhf, 0.95);
    result.isSignificant = PearsonCorrelation.isSignificant(result.estimatedCorrelation, nhf, .05);
    result.nhf = nhf;
  }

  public static class Result {

    public String columnId;
    // kmv estimation statistics
    public double jcx;
    public double jcy;
    public double jsx;
    public double jsy;
    public double cardinalityQ;
    public double cardinalityC;
    public double actualCardinalityQ;
    public double actualCardinalityC;
    // person estimation statistics
    public int nhf;
    public double actualCorrelation;
    public double estimatedCorrelation;
    public double deltaCorrelation;
    public double pValueTwoTailed;
    public ConfidenceInterval confidenceInterval;
    public boolean isSignificant;

    public static String header() {
      return String.format(
          "Contain  Pearson  Estimation  Error  CardQ  CardC  p-value  Interval           Sig    Name");
    }

    public static String csvHeader() {
      return String.format(
          "jcx,jcy,jsx,jsy,pearson,pearson_est,pearson_error,cardq,cardq_actual,cardc,cardc_actual,pvalue,interval,significance,columnpair");
    }

    public String csvLine() {
      return String.format(
          "%.3f,%.3f,%.3f,%.3f,%.2f,%.2f,%.2f,%.2f,%.2f,%.2f,%.2f,%.3f,%s,%s,%s",
          jcx,
          jcy,
          jsx,
          jsy,
          actualCorrelation,
          estimatedCorrelation,
          deltaCorrelation,
          cardinalityQ,
          actualCardinalityQ,
          cardinalityC,
          actualCardinalityC,
          pValueTwoTailed,
          StringEscapeUtils.escapeCsv(String.valueOf(confidenceInterval)),
          StringEscapeUtils.escapeCsv(String.valueOf(isSignificant)),
          StringEscapeUtils.escapeCsv(columnId));
    }

    @Override
    public String toString() {
      return String.format(
          "%+.4f  %+.4f  %+.7f  %+.2f  %.2f   %.2f   %.3f    %-17s  %-5s  %s\n",
          jcx,
          actualCorrelation,
          estimatedCorrelation,
          deltaCorrelation,
          cardinalityQ,
          cardinalityC,
          pValueTwoTailed,
          confidenceInterval,
          isSignificant,
          columnId);
    }
  }
}
