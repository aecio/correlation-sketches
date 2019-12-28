package benchmark;

import com.github.rvesse.airline.annotations.Command;
import com.github.rvesse.airline.annotations.Option;
import com.github.rvesse.airline.annotations.restrictions.Required;
import it.unimi.dsi.fastutil.doubles.DoubleArrayList;
import it.unimi.dsi.fastutil.doubles.DoubleList;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import sketches.correlation.KMVCorrelationSketch;
import sketches.correlation.PearsonCorrelation;
import sketches.correlation.SketchIndex;
import sketches.correlation.SketchIndex.Hit;
import benchmark.BenchmarkUtils.Result;
import sketches.correlation.Sketches;
import sketches.correlation.Sketches.Type;
import sketches.kmv.IKMV;
import sketches.kmv.KMV;
import spark.ComputePairwiseCorrelationJoins;
import utils.CliTool;

@Command(
    name = IndexCorrelationBenchmark.JOB_NAME,
    description = "Creates a Lucene index of tables")
public class IndexCorrelationBenchmark extends CliTool implements Serializable {

  public static final String JOB_NAME = "IndexCorrelationBenchmark";

  @Required
  @Option(name = "--input-path", description = "Folder containing CSV files")
  private String inputPath;

  @Required
  @Option(name = "--output-path", description = "Output path for results file")
  private String outputPath;

  @Option(name = "--sketch-type", description = "The type sketch to be used")
  private Type sketchType = Type.KMV;

  @Required
  @Option(name = "--num-hashes", description = "Number of hashes per sketch")
  private double numHashes = KMV.DEFAULT_K;

  @Option(name = "--min-rows", description = "Minimum number of rows to consider table")
  int minRows = 1;

  public static void main(String[] args) {
    CliTool.run(args, new ComputePairwiseCorrelationJoins());
  }

  @Override
  public void execute() {

    String basePath = "/home/aeciosantos/workspace/d3m/data-search-datasets/poverty-data";

    List<String> allFiles = BenchmarkUtils.findAllCSVs(inputPath);
    Set<ColumnPair> allColumns = BenchmarkUtils.readAllColumnPairs(allFiles, minRows);

    DoubleList estimationsCorrelations = new DoubleArrayList();
    int minHashFunctionsExp = 8;
    for (int k = minHashFunctionsExp; k <= 8; k++) {

      int nhf = (int) Math.pow(2, k);
      System.out.printf("\nCorrelation Sketch with %d hash functions:\n\n", nhf);
      //            MinwiseHasher minHasher = new MinwiseHasher(nhf);

      Map<String, ColumnPair> idToColumnMap = new HashMap<>();

      SketchIndex index = new SketchIndex();
      for (ColumnPair column : allColumns) {
        KMV kmv = KMV.create(column.keyValues, column.columnValues, nhf);
        KMVCorrelationSketch columnSketch = new KMVCorrelationSketch(kmv);
        index.index(column.id(), columnSketch);
        idToColumnMap.put(column.id(), column);
      }

      System.out.println(Result.header());

      List<Result> results = new ArrayList<>();

      for (ColumnPair query : allColumns) {

        KMVCorrelationSketch querySketch =
            new KMVCorrelationSketch(query.keyValues, query.columnValues, nhf);
        long start = System.currentTimeMillis();
        List<Hit> hits = index.search(querySketch, 10);
        long end = System.currentTimeMillis();
        System.err.println(end - start);

        for (Hit hit : hits) {
          if (query.id().equals(hit.id)) {
            continue;
          }

          KMVCorrelationSketch columnSketch = hit.sketch;

          Result result = new Result();

          result.corr_est = querySketch.correlationTo(columnSketch).coefficient;
          if (Double.isNaN(result.corr_est)) {
            continue;
          }

          ColumnPair column = idToColumnMap.get(hit.id);
          result.corr_actual = Tables.computePearsonAfterJoin(query, column);
          if (Double.isNaN(result.corr_actual)) {
            continue;
          }

          result.columnId =
              String.format(
                  "q(%s,%s)<->c(%s,%s)",
                  query.keyName, query.columnName, column.keyName, column.columnName);
          int actualCardinalityQ = new HashSet<>(query.keyValues).size();
          int actualCardinalityC = new HashSet<>(column.keyValues).size();

          //          BenchmarkUtils.computeStatistics(
          //              nhf, result, querySketch, columnSketch, actualCardinalityQ,
          // actualCardinalityC);
          results.add(result);
        }
      }

      //            results.sort((a, b) -> {
      //                return Double.compare(
      //                        Math.abs(a.actualCorrelation - a.estimatedCorrelation),
      //                        Math.abs(b.actualCorrelation - b.estimatedCorrelation)
      //                );
      //            });
      //            results.sort((a, b) -> Double.compare(b.containment, a.containment));
      //            results.sort((a, b) -> Double.compare(b.estimatedCorrelation,
      // a.estimatedCorrelation));
      results.sort((a, b) -> Double.compare(Math.abs(b.corr_actual), Math.abs(a.corr_actual)));

      for (Result result : results) {
        System.out.printf(result.toString());
      }

      double[] estimation = results.stream().mapToDouble(r -> r.corr_est).toArray();

      double[] actual = results.stream().mapToDouble(r -> r.corr_actual).toArray();

      estimationsCorrelations.add(PearsonCorrelation.coefficient(estimation, actual));
      System.out.println();
    }

    System.out.println("Pearson  #-murmur3_32  p-value  Interval          Significance\n");
    for (int k = minHashFunctionsExp; k <= 8; k++) {
      int nhf = (int) Math.pow(2, k);
      double corr = estimationsCorrelations.getDouble(k - minHashFunctionsExp);
      System.out.printf(
          "%+.4f  %-8d  %-7.3f  %s  %s\n",
          corr,
          nhf,
          PearsonCorrelation.pValueOneTailed(corr, nhf),
          PearsonCorrelation.confidenceInterval(corr, nhf, 0.95),
          PearsonCorrelation.isSignificant(corr, nhf, .05));
    }
  }
}
