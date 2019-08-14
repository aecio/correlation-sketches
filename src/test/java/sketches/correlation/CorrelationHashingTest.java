package sketches.correlation;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import org.junit.Assert;
import org.junit.Test;
import tech.tablesaw.api.ColumnType;
import tech.tablesaw.api.NumericColumn;
import tech.tablesaw.api.Table;
import tech.tablesaw.columns.Column;

public class CorrelationHashingTest {

    @Test
    public void shouldEstimateCorrelation() {
        List<String> pk = Arrays.asList(new String[]{"a", "b", "c", "d"});
        double[] q1 = new double[]{1.0, 2.0, 3.0, 4.0, 5.0};

        List<String> fk = Arrays.asList(new String[]{"a", "b", "c", "d"});
        double[] c0 = new double[]{1.0, 2.0, 3.0, 4.0, 5.0};
        double[] c1 = new double[]{1.1, 2.5, 3.0, 4.4, 5.9};
        double[] c2 = new double[]{1.0, 3.2, 3.1, 4.9, 5.4};

        CorrelationSketch q1sk = new CorrelationSketch(pk, q1);
        CorrelationSketch c0sk = new CorrelationSketch(fk, c0);
        CorrelationSketch c1sk = new CorrelationSketch(fk, c1);
        CorrelationSketch c2sk = new CorrelationSketch(fk, c2);

        double delta = 0.001;
        Assert.assertEquals(1.000, q1sk.correlationTo(q1sk), delta);
        Assert.assertEquals(1.000, q1sk.correlationTo(c0sk), delta);
        Assert.assertEquals(0.981, q1sk.correlationTo(c1sk), delta);
        Assert.assertEquals(0.924, q1sk.correlationTo(c2sk), delta);
    }

    @Test
    public void shouldRankFeaturesFromCSV() throws IOException {
        String datasetPath = "/home/aeciosantos/workspace/d3m/datasets/seed_datasets_current/LL0_207_autoPrice/LL0_207_autoPrice_dataset/";
        String datasetTable = "tables/learningData.csv";

        Table df = Table.read().csv(datasetPath + datasetTable);
        System.out.println("row count: " + df.rowCount());
        Column target = df.column("class");
        Column key = df.column("d3mIndex");
        List<String> keyValues = key.asStringColumn().asList();

        df.removeColumns("class");
        df.removeColumns("d3mIndex");


        double[] targetValues = doubleArray(target);

        double[] estimationsCorrelations = new double[8];
        int minHashFunctionsExp = 3;
        for(int k = minHashFunctionsExp; k <= 8; k++) {
            int nhf = (int) Math.pow(2, k);
            System.out.printf("\nCorrelation Sketch with %d hash functions:\n\n", nhf);
            MinwiseHasher minHasher = new MinwiseHasher(nhf);

            CorrelationSketch targetSketch = new CorrelationSketch(keyValues, targetValues, minHasher);

            double[] sketchCorrelations = new double[targetValues.length];
            double[] pearsonCorrelations = new double[targetValues.length];
            int i = 0;
            System.out.println("Pearson  Estimation  p-value  Interval           Sig    Name");
            for (NumericColumn column : df.numericColumns()) {
                double[] columnValues = column.asDoubleArray();
                CorrelationSketch columnSketch = new CorrelationSketch(keyValues, columnValues, minHasher);
                sketchCorrelations[i] = targetSketch.correlationTo(columnSketch);
                pearsonCorrelations[i] = PearsonCorrelation.coefficient(targetValues, columnValues);

                System.out.printf(
                    "%+.4f  %+.4f     %.3f    %-17s  %-5s  %s\n",
                    pearsonCorrelations[i],
                    sketchCorrelations[i],
                    PearsonCorrelation.pValueTwoTailed(pearsonCorrelations[i], nhf),
                    PearsonCorrelation.confidenceInterval(pearsonCorrelations[i], nhf, 0.95),
                    PearsonCorrelation.isSignificant(pearsonCorrelations[i], nhf, .05),
                    column.name()
                );
                i++;
            }
            estimationsCorrelations[k-1] = PearsonCorrelation.coefficient(sketchCorrelations, pearsonCorrelations);
            System.out.println();
        }

        System.out.println("Pearson  #-hashes  p-value  Interval          Significance\n");
        for(int k = minHashFunctionsExp; k <= 8; k++) {
            int nhf = (int) Math.pow(2, k);
            System.out.printf("%+.4f  %-8d  %-7.3f  %s  %s\n",
                estimationsCorrelations[k-1],
                nhf,
                PearsonCorrelation.pValueOneTailed(estimationsCorrelations[k-1], nhf),
                PearsonCorrelation.confidenceInterval(estimationsCorrelations[k-1], nhf, 0.95),
                PearsonCorrelation.isSignificant(estimationsCorrelations[k-1], nhf, .05)
            );
        }

    }

    public static double[] doubleArray(Column column) {
        double[] array = new double[column.size()];
        if (column.type() == ColumnType.INTEGER) {
            Integer[] ints = (Integer[]) column.asObjectArray();
            for (int i = 0; i < ints.length; i++) {
                array[i] = ints[i];
            }
        } else if(column.type() == ColumnType.DOUBLE) {
            Double[] doubles = (Double[]) column.asObjectArray();
            for (int i = 0; i < doubles.length; i++) {
                array[i] = doubles[i];
            }
        } else {
            throw new IllegalArgumentException(
                String.format("Column of type %s can't be cast to double[]", column.type().toString())
            );
        }
        return array;
    }
}
