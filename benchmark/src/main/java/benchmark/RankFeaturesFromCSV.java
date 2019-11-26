package benchmark;

import java.io.IOException;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sketches.correlation.KMVCorrelationSketch;
import sketches.correlation.PearsonCorrelation;
import tech.tablesaw.api.CategoricalColumn;
import tech.tablesaw.api.NumericColumn;
import tech.tablesaw.api.Table;
import tech.tablesaw.columns.Column;

public class RankFeaturesFromCSV {

  @SuppressWarnings("unused")
  private static final Logger logger = LoggerFactory.getLogger(RankFeaturesFromCSV.class);

  public static void main(String[] args) throws IOException {
    String datasetPath =
        "/home/aeciosantos/workspace/d3m/datasets/seed_datasets_current/LL0_207_autoPrice/LL0_207_autoPrice_dataset/";
    String datasetTable = "tables/learningData.csv";
    String targetAttribute = "class";

    Table df = Table.read().csv(datasetPath + datasetTable);
    System.out.println("row count: " + df.rowCount());

    Column<?> target = df.column(targetAttribute);
    Column<?> key = df.column("d3mIndex");

    df.removeColumns(targetAttribute);
    df.removeColumns("d3mIndex");

    ColumnPair columnPair =
        Tables.createColumnPair(datasetPath, (CategoricalColumn<?>) key, (NumericColumn<?>) target);

    List<String> keyValues = columnPair.keyValues;
    double[] targetValues = columnPair.columnValues;

    double[] estimationsCorrelations = new double[8];
    int minHashFunctionsExp = 3;
    for (int k = minHashFunctionsExp; k <= 8; k++) {
      int nhf = (int) Math.pow(2, k);
      System.out.printf("\nCorrelation Sketch with %d hash functions:\n\n", nhf);

      //            MinwiseHasher minHasher = new MinwiseHasher(nhf);
      //            MinhashCorrelationSketch targetSketch = new MinhashCorrelationSketch(keyValues,
      // targetValues, minHasher);
      KMVCorrelationSketch targetSketch = new KMVCorrelationSketch(keyValues, targetValues, nhf);

      double[] sketchCorrelations = new double[targetValues.length];
      double[] pearsonCorrelations = new double[targetValues.length];
      int i = 0;
      System.out.println("Pearson  Estimation  p-value  Interval           Sig    Name");
      for (NumericColumn<?> column : df.numericColumns()) {
        double[] columnValues = column.asDoubleArray();
        //                MinhashCorrelationSketch columnSketch = new
        // MinhashCorrelationSketch(keyValues, columnValues, minHasher);
        KMVCorrelationSketch columnSketch = new KMVCorrelationSketch(keyValues, columnValues, nhf);
        sketchCorrelations[i] = targetSketch.correlationTo(columnSketch).coefficient;
        pearsonCorrelations[i] = PearsonCorrelation.coefficient(targetValues, columnValues);

        System.out.printf(
            "%+.4f  %+.4f     %.3f    %-17s  %-5s  %s\n",
            pearsonCorrelations[i],
            sketchCorrelations[i],
            PearsonCorrelation.pValueTwoTailed(pearsonCorrelations[i], nhf),
            PearsonCorrelation.confidenceInterval(pearsonCorrelations[i], nhf, 0.95),
            PearsonCorrelation.isSignificant(pearsonCorrelations[i], nhf, .05),
            column.name());
        i++;
      }
      estimationsCorrelations[k - 1] =
          PearsonCorrelation.coefficient(sketchCorrelations, pearsonCorrelations);
      System.out.println();
    }

    System.out.println("Pearson  #-murmur3_32  p-value  Interval          Significance\n");
    for (int k = minHashFunctionsExp; k <= 8; k++) {
      int nhf = (int) Math.pow(2, k);
      System.out.printf(
          "%+.4f  %-8d  %-7.3f  %s  %s\n",
          estimationsCorrelations[k - 1],
          nhf,
          PearsonCorrelation.pValueOneTailed(estimationsCorrelations[k - 1], nhf),
          PearsonCorrelation.confidenceInterval(estimationsCorrelations[k - 1], nhf, 0.95),
          PearsonCorrelation.isSignificant(estimationsCorrelations[k - 1], nhf, .05));
    }
  }
}
