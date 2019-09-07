package sketches.correlation.benchmark;

import it.unimi.dsi.fastutil.doubles.DoubleArrayList;
import it.unimi.dsi.fastutil.doubles.DoubleList;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sketches.correlation.KMVCorrelationSketch;
import sketches.correlation.PearsonCorrelation;
import sketches.correlation.benchmark.BenchmarkUtils.Result;


public class CorrelationBenchmark {

    @SuppressWarnings("unused")
    private static final Logger logger = LoggerFactory.getLogger(CorrelationBenchmark.class);

    public static void main(String[] args) throws IOException {

        String basePath = "/home/aeciosantos/workdata/socrata/data/finances.worldbank.org";
//        String basePath = "/home/aeciosantos/workspace/d3m/datasets/seed_datasets_current/";

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
                    if(query == column) {
                        continue;
                    }

//                    MinhashCorrelationSketch querySketch = new MinhashCorrelationSketch(
//                            query.keyValues,
//                            query.columnValues,
//                            minHasher
//                    );
//                    MinhashCorrelationSketch columnSketch = new MinhashCorrelationSketch(
//                            column.keyValues,
//                            column.columnValues,
//                            minHasher
//                    );
                    KMVCorrelationSketch querySketch = new KMVCorrelationSketch(
                            query.keyValues,
                            query.columnValues,
                            nhf
                    );

                    KMVCorrelationSketch columnSketch = new KMVCorrelationSketch(
                            column.keyValues,
                            column.columnValues,
                            nhf
                    );

                    Result result = new Result();

                    result.estimatedCorrelation = querySketch.correlationTo(columnSketch);
                    if(Double.isNaN(result.estimatedCorrelation)) {
                        continue;
                    }

                    result.actualCorrelation = Tables.computePearsonAfterJoin(query, column);
                    if(Double.isNaN(result.actualCorrelation)) {
                        continue;
                    }

                    result.columnId = String.format("q(%s,%s)<->c(%s,%s)", query.keyName, query.columnName, column.keyName, column.columnName);
                    BenchmarkUtils.computeStatistics(nhf, result, querySketch, columnSketch);
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
//            results.sort((a, b) -> Double.compare(b.estimatedCorrelation, a.estimatedCorrelation));
            results.sort((a, b) -> Double.compare(Math.abs(b.actualCorrelation), Math.abs(a.actualCorrelation)));

            for (Result result : results) {
                System.out.printf(result.toString());
            }

            double[] estimation = results.stream()
                    .mapToDouble(r -> r.estimatedCorrelation)
                    .toArray();

            double[] actual = results.stream()
                    .mapToDouble(r -> r.actualCorrelation)
                    .toArray();

            estimationsCorrelations.add(PearsonCorrelation.coefficient(estimation, actual));
            System.out.println();
        }

        System.out.println("Pearson  #-murmur3_32  p-value  Interval          Significance\n");
        for (int k = minHashFunctionsExp; k <= 8; k++) {
            int nhf = (int) Math.pow(2, k);
            double corr = estimationsCorrelations.getDouble(k - minHashFunctionsExp);
            System.out.printf("%+.4f  %-8d  %-7.3f  %s  %s\n",
                    corr,
                    nhf,
                    PearsonCorrelation.pValueOneTailed(corr, nhf),
                    PearsonCorrelation.confidenceInterval(corr, nhf, 0.95),
                    PearsonCorrelation.isSignificant(corr, nhf, .05)
            );
        }
    }

}
