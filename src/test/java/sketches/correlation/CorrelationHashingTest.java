package sketches.correlation;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import org.junit.Assert;
import org.junit.Test;
import sketches.correlation.benchmark.Tables;
import tech.tablesaw.api.NumericColumn;
import tech.tablesaw.api.Table;
import tech.tablesaw.columns.Column;

public class CorrelationHashingTest {

    @Test
    public void test() {
        List<String> pk = Arrays.asList(new String[]{"a", "b", "c", "d", "e"});
        double[] q = new double[]{1.0, 2.0, 3.0, 4.0, 5.0};

        KMVCorrelationSketch qsk = new KMVCorrelationSketch(pk, q);

        double delta = 0.1;

        List<String> c4fk = Arrays.asList(new String[]{"a", "b", "c", "z", "x"});
        double[] c4 = new double[]{1.0, 2.0, 3.0, 4.0, 5.0};
//        List<String> c4fk = Arrays.asList(new String[]{"a", "b", "c", "d"});
//        double[] c4 = new double[]{1.0, 2.0, 3.0, 4.0};

        KMVCorrelationSketch c4sk = new KMVCorrelationSketch(c4fk, c4);
        System.out.println();
        System.out.println("         union: " + qsk.unionSize(c4sk));
        System.out.println("  intersection: " + qsk.intersectionSize(c4sk));
        System.out.println("       jaccard: " + qsk.jaccard(c4sk));
        System.out.println("cardinality(x): " + qsk.cardinality());
        System.out.println("cardinality(y): " + c4sk.cardinality());
        System.out.println("containment(x): " + qsk.containment(c4sk));
        System.out.println("containment(y): " + c4sk.containment(qsk));
        System.out.flush();
        System.err.flush();
        c4sk.setCardinality(5);
        qsk.setCardinality(5);
        System.out.println();
        System.out.println("         union: " + qsk.unionSize(c4sk));
        System.out.println("  intersection: " + qsk.intersectionSize(c4sk));
        System.out.println("       jaccard: " + qsk.jaccard(c4sk));
        System.out.println("cardinality(x): " + qsk.cardinality());
        System.out.println("cardinality(y): " + c4sk.cardinality());
        System.out.println("containment(x): " + qsk.containment(c4sk));
        System.out.println("containment(y): " + c4sk.containment(qsk));
    }

    @Test
    public void shouldEstimateCorrelationUsingKMVSketch() {
        List<String> pk = Arrays.asList(new String[]{"a", "b", "c", "d", "e"});
        double[] q = new double[]{1.0, 2.0, 3.0, 4.0, 5.0};

        List<String> fk = Arrays.asList(new String[]{"a", "b", "c", "d", "e"});
        double[] c0 = new double[]{1.0, 2.0, 3.0, 4.0, 5.0};
        double[] c1 = new double[]{1.1, 2.5, 3.0, 4.4, 5.9};
        double[] c2 = new double[]{1.0, 3.2, 3.1, 4.9, 5.4};

        KMVCorrelationSketch qsk = new KMVCorrelationSketch(pk, q);
        KMVCorrelationSketch c0sk = new KMVCorrelationSketch(fk, c0);
        KMVCorrelationSketch c1sk = new KMVCorrelationSketch(fk, c1);
        KMVCorrelationSketch c2sk = new KMVCorrelationSketch(fk, c2);

        double delta = 0.1;
        Assert.assertEquals(1.000, qsk.correlationTo(qsk), delta);
        Assert.assertEquals(1.000, qsk.correlationTo(c0sk), delta);
        Assert.assertEquals(0.9895, qsk.correlationTo(c1sk), delta);
        Assert.assertEquals(0.9558, qsk.correlationTo(c2sk), delta);
    }

    @Test
    public void shouldEstimateCorrelation() {
        List<String> pk = Arrays.asList(new String[]{"a", "b", "c", "d", "e"});
        double[] q1 = new double[]{1.0, 2.0, 3.0, 4.0, 5.0};

        List<String> fk = Arrays.asList(new String[]{"a", "b", "c", "d", "e"});
        double[] c0 = new double[]{1.0, 2.0, 3.0, 4.0, 5.0};
        double[] c1 = new double[]{1.1, 2.5, 3.0, 4.4, 5.9};
        double[] c2 = new double[]{1.0, 3.2, 3.1, 4.9, 5.4};

        MinhashCorrelationSketch q1sk = new MinhashCorrelationSketch(pk, q1);
        MinhashCorrelationSketch c0sk = new MinhashCorrelationSketch(fk, c0);
        MinhashCorrelationSketch c1sk = new MinhashCorrelationSketch(fk, c1);
        MinhashCorrelationSketch c2sk = new MinhashCorrelationSketch(fk, c2);

        double delta = 0.005;
        Assert.assertEquals(1.000, q1sk.correlationTo(q1sk), delta);
        Assert.assertEquals(1.000, q1sk.correlationTo(c0sk), delta);
        Assert.assertEquals(0.987, q1sk.correlationTo(c1sk), delta);
        Assert.assertEquals(0.947, q1sk.correlationTo(c2sk), delta);
    }

    @Test
    public void shouldRankFeaturesFromCSV() throws IOException {
        String datasetPath = "/home/aeciosantos/workspace/d3m/datasets/seed_datasets_current/LL0_207_autoPrice/LL0_207_autoPrice_dataset/";
        String datasetTable = "tables/learningData.csv";
        String targetAttribute = "class";

        Table df = Table.read().csv(datasetPath + datasetTable);
        System.out.println("row count: " + df.rowCount());

        Column target = df.column(targetAttribute);
        Column key = df.column("d3mIndex");
        List<String> keyValues = key.asStringColumn().asList();

        df.removeColumns(targetAttribute);
        df.removeColumns("d3mIndex");


        double[] targetValues = Tables.doubleArray(target);

        double[] estimationsCorrelations = new double[8];
        int minHashFunctionsExp = 3;
        for(int k = minHashFunctionsExp; k <= 8; k++) {
            int nhf = (int) Math.pow(2, k);
            System.out.printf("\nCorrelation Sketch with %d hash functions:\n\n", nhf);

//            MinwiseHasher minHasher = new MinwiseHasher(nhf);
//            MinhashCorrelationSketch targetSketch = new MinhashCorrelationSketch(keyValues, targetValues, minHasher);
            KMVCorrelationSketch targetSketch = new KMVCorrelationSketch(keyValues, targetValues, nhf);

            double[] sketchCorrelations = new double[targetValues.length];
            double[] pearsonCorrelations = new double[targetValues.length];
            int i = 0;
            System.out.println("Pearson  Estimation  p-value  Interval           Sig    Name");
            for (NumericColumn column : df.numericColumns()) {
                double[] columnValues = column.asDoubleArray();
//                MinhashCorrelationSketch columnSketch = new MinhashCorrelationSketch(keyValues, columnValues, minHasher);
                KMVCorrelationSketch columnSketch = new KMVCorrelationSketch(keyValues, columnValues, nhf);
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

        System.out.println("Pearson  #-murmur3_32  p-value  Interval          Significance\n");
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

    @Test
    public void shouldRankFeaturesFromDatasets() throws IOException {


    }

}
