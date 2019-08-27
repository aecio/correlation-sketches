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

            System.err.println("dataset: " + dataset);
            Table df = Table.read().csv(CsvReadOptions.builder(dataset).maxCharsPerColumn(10000));
//            df.columnNames().contains("d3mIndex")

            System.out.printf("Row count: %d  File: %s\n", df.rowCount(), dataset);

            List<StringColumn> categoricalColumns = Arrays.asList(df.stringColumns());
            System.out.println("Categorical columns: " + categoricalColumns.size());

            List<NumericColumn<?>> numericColumns = df.numericColumns();
            System.out.println("Numeric columns: " + numericColumns.size());

            for (CategoricalColumn<?> key : categoricalColumns) {

                String keyName = key.name();
                List<String> keyValues = key.asStringColumn().asList();

                for (NumericColumn column : numericColumns) {
                    try {
                        double[] columnValues = Tables.doubleArray(column);
                        String columnName = column.name();
                        ColumnPair columnPair = new ColumnPair(dataset,
                                keyName,
                                keyValues,
                                columnName,
                                columnValues
                        );
                        allPairs.add(columnPair);
                    } catch (Exception e) {
                        e.printStackTrace();
                        continue;
                    }
                }
            }
        }
        return allPairs;
    }

    public static List<String> findAllCSVs(String basePath) throws IOException {
//        List<String> datasetsIDs = Arrays.asList(
//                "534_cps_85_wages",
//                "LL0_1100_popularkids",
////                "26_radon_seed",
//                "185_baseball",
//                "LL0_207_autoPrice"
//        );
//        String template = "/home/aeciosantos/workspace/d3m/datasets/seed_datasets_current/%s/%s_dataset/tables/learningData.csv";
//        String template = "/home/aeciosantos/workspace/d3m/demo-datasets/%s/%s_dataset/tables/learningData.csv";

//        List<String> allFiles = datasetsIDs.stream().map(d -> String.format(template, d, d)).collect(Collectors
//                .toList());

//        List<String> allFiles = Files.walk(Paths.get(basePath))
//                .filter(p -> p.endsWith(Paths.get(datasetTable)))
//                .map(p -> p.toAbsolutePath().toString())
//                .collect(Collectors.toList()
//        );
        List<String> allFiles = Files.walk(Paths.get(basePath))
                .filter(p -> Files.isRegularFile(p))
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
