package corrsketches.correlation;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.byLessThan;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import org.junit.jupiter.api.Test;

public class KendallTauCorrelationTest {

  @Test
  public void shouldComputeCorrelationCoefficient() {
    double[] x =
        new double[] {
          60323, 61122, 60171, 61187, 63221, 63639, 64989, 63761, 66019, 67857, 68169, 66513, 68655,
          69564, 69331, 70551
        };
    double[] y =
        new double[] {
          83.0, 88.5, 88.2, 89.5, 96.2, 98.1, 99.0, 100.0, 101.2, 104.6, 108.4, 110.8, 112.6, 114.2,
          115.7, 116.9
        };
    double[] z =
        new double[] {
          234289, 259426, 258054, 284599, 328975, 346999, 365385, 363112, 397469, 419180, 442769,
          444546, 482704, 502601, 518173, 554894,
        };
    assertThat(KendallTauCorrelation.correlation(x, y))
        .isEqualTo(0.9166666666666666d, byLessThan(10E-15));
    assertThat(KendallTauCorrelation.correlation(x, z))
        .isEqualTo(0.9333333333333332d, byLessThan(10E-15));
  }

  @Test
  public void testMath1277() {
    // example that led to a correlation coefficient outside of [-1, 1]
    // due to a bug reported in MATH-1277
    Random rng = new Random();
    double[] xArray = new double[120000];
    double[] yArray = new double[120000];
    for (int i = 0; i < xArray.length; ++i) {
      xArray[i] = rng.nextDouble();
    }
    for (int i = 0; i < yArray.length; ++i) {
      yArray[i] = rng.nextDouble();
    }
    double coefficient = KendallTauCorrelation.correlation(xArray, yArray);
    assertThat(1.0 >= coefficient && -1.0 <= coefficient).isTrue();
  }

  @Test
  public void testMI() throws Exception {

    double[][] X = readDoublesCSV(Paths.get(this.getClass().getResource("/mi_test_x.csv").toURI()));
    double[] y =
        readDoublesCSV(Paths.get(this.getClass().getResource("/mi_test_y.csv").toURI()))[0];

    System.out.println(Arrays.toString(y));
    double[] taus = new double[X.length];
    double[] mis = new double[X.length];
    double maxMi = 0;
    for (int i = 0; i < X.length; i++) {
      taus[i] = KendallTauCorrelation.correlation(X[i], y);
      mis[i] = KendallTauCorrelation.kendallToMI(taus[i]);
      maxMi = Math.max(maxMi, mis[i]);
    }

    for (int i = 0; i < X.length; i++) {
      double tau = taus[i];
      double mi = mis[i];
      System.out.printf(
          "tau=%+.5f mi=%.5f max_mi=%.5f, mi_log2=%.5f\n", tau, mi, mi / maxMi, mi / Math.log(2));
    }
  }

  public static double[][] readDoublesCSV(Path filePath) throws IOException {
    List<String> lines = Files.readAllLines(filePath);
    int ncolumns = lines.get(0).split(",").length;
    double[][] data = new double[ncolumns][lines.size()];
    for (int i = 0; i < ncolumns; i++) {
      data[i] = new double[lines.size()];
    }
    for (int j = 0, linesSize = lines.size(); j < linesSize; j++) {
      String line = lines.get(j);
      String[] cols = line.split(",");
      if (cols.length != ncolumns) {
        throw new IllegalArgumentException(
            "Number of columns in line %d do not match number of columns of the first row");
      }
      for (int i = 0; i < ncolumns; i++) {
        data[i][j] = Double.parseDouble(cols[i]);
      }
    }
    System.out.printf("%s %s\n", ncolumns, lines.size());
    return data;
  }
}
