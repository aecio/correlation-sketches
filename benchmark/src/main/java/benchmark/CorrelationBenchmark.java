package benchmark;

import it.unimi.dsi.fastutil.doubles.DoubleArrayList;
import it.unimi.dsi.fastutil.doubles.DoubleList;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sketches.correlation.PearsonCorrelation;
import benchmark.BenchmarkUtils.Result;

public class CorrelationBenchmark {

  @SuppressWarnings("unused")
  private static final Logger logger = LoggerFactory.getLogger(CorrelationBenchmark.class);

  public static void main(String[] args) throws IOException {

    //        String basePath = "/home/aeciosantos/workdata/socrata/data/finances.worldbank.org";
    String basePath = "/home/aeciosantos/workspace/d3m/datasets/seed_datasets_current/";

    List<String> allFiles = BenchmarkUtils.findAllCSVs(basePath);
    System.out.printf("Found %d files\n", allFiles.size());

    Set<ColumnPair> allColumns = BenchmarkUtils.readAllColumnPairs(allFiles);

    DoubleList estimationsCorrelations = new DoubleArrayList();
    int minHashFunctionsExp = 8;
    for (int k = minHashFunctionsExp; k <= 8; k++) {

      int nhf = (int) Math.pow(2, k);
      System.out.printf("\nCorrelation Sketch with %d hash functions:\n\n", nhf);
      // MinwiseHasher minHasher = new MinwiseHasher(nhf);

      System.out.println(Result.header());

      List<Result> results = new ArrayList<>();
      for (ColumnPair query : allColumns) {
        for (ColumnPair column : allColumns) {
          if (query == column) {
            continue;
          }

          Result result = BenchmarkUtils.computeStatistics(nhf, query, column);
          if (result == null) {
            continue;
          }
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
      results.sort(
          (a, b) -> Double.compare(Math.abs(b.corr_actual), Math.abs(a.corr_actual)));

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
