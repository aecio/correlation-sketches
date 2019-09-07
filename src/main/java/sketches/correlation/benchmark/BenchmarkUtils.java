package sketches.correlation.benchmark;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import sketches.correlation.KMVCorrelationSketch;
import sketches.correlation.PearsonCorrelation;
import sketches.correlation.PearsonCorrelation.ConfidenceInterval;
import tech.tablesaw.api.CategoricalColumn;
import tech.tablesaw.api.NumericColumn;
import tech.tablesaw.api.StringColumn;
import tech.tablesaw.api.Table;
import tech.tablesaw.io.csv.CsvReadOptions;

public class BenchmarkUtils {

    public static Set<ColumnPair> readAllColumnPairs(List<String> allFiles)
            throws IOException {
        Set<ColumnPair> allPairs = new HashSet<>();
        for (int i = 0; i < allFiles.size(); i++) {
            String dataset = allFiles.get(i);
            allPairs.addAll(readColumnPairs(dataset));
        }
        return allPairs;
    }

    private static Set<ColumnPair> readColumnPairs(String dataset) throws IOException {
        Set<ColumnPair> pairs = new HashSet<>();
        System.err.println("dataset: " + dataset);
        Table df = Table.read().csv(
            CsvReadOptions.builder(dataset)
                .maxCharsPerColumn(10000)
                .missingValueIndicator("-")
        );

        System.out.printf("Row count: %d  File: %s\n", df.rowCount(), dataset);

        List<StringColumn> categoricalColumns = Arrays.asList(df.stringColumns());
        System.out.println("Categorical columns: " + categoricalColumns.size());

        List<NumericColumn<?>> numericColumns = df.numericColumns();
        System.out.println("Numeric columns: " + numericColumns.size());

        for (CategoricalColumn<?> key : categoricalColumns) {
            for (NumericColumn<?> column : numericColumns) {
                try {
                    pairs.add(Tables.createColumnPair(dataset, key, column));
                } catch (Exception e) {
                    e.printStackTrace();
                    continue;
                }
            }
        }
        return pairs;
    }

    public static List<String> findAllCSVs(String basePath) throws IOException {

        List<String> allFiles = Files.walk(Paths.get(basePath))
                .filter(p -> p.toString().endsWith(".csv"))
                .filter(Files::isRegularFile)
                .map(p -> p.toAbsolutePath().toString())
                .collect(Collectors.toList()
        );

        return allFiles;
    }

    public static void computeStatistics(int nhf, Result result,
            KMVCorrelationSketch querySketch, KMVCorrelationSketch columnSketch) {
        result.containment = querySketch.containment(columnSketch);
        result.cardinalityQ = querySketch.cardinality();
        result.cardinalityC = columnSketch.cardinality();
        result.pValueTwoTailed = PearsonCorrelation.pValueTwoTailed(result.estimatedCorrelation, nhf);
        result.confidenceInterval = PearsonCorrelation.confidenceInterval(result.estimatedCorrelation, nhf, 0.95);
        result.isSignificant = PearsonCorrelation.isSignificant(result.estimatedCorrelation, nhf, .05);
        result.nhf = nhf;
    }


    public static class Result {

        protected int nhf;
        protected double containment;
        protected double actualCorrelation;
        protected double estimatedCorrelation;
        protected double cardinalityQ;
        protected double cardinalityC;
        protected double pValueTwoTailed;
        protected ConfidenceInterval confidenceInterval;
        protected boolean isSignificant;
        protected String columnId;

        public static String header() {
            return String.format("Contain  Pearson  Estimation  Error  CardQ  CardC  p-value  Interval           Sig    Name");
        }

        @Override
        public String toString() {
            return String.format(
                    "%+.4f  %+.4f  %+.7f  %+.2f  %.2f   %.2f   %.3f    %-17s  %-5s  %s\n",
                    containment,
                    actualCorrelation,
                    estimatedCorrelation,
                    actualCorrelation - estimatedCorrelation,
                    cardinalityQ,
                    cardinalityC,
                    pValueTwoTailed,
                    confidenceInterval,
                    isSignificant,
                    columnId
            );
        }
    }

}
