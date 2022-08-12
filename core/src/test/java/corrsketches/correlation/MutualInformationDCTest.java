package corrsketches.correlation;

import static org.assertj.core.api.Assertions.*;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Random;
import org.junit.jupiter.api.Test;

public class MutualInformationDCTest {

  final double DELTA = 0.0001;

  @Test
  public void testDiscreteContinuousMI() {
    int k = 3;
    double base = Math.exp(1);
    int[] d1 = new int[] {1, 1, 2, 2, 3, 3};

    //
    // Case 1
    //
    double[] c1 = new double[] {1.0, 1.0, 2.3, 2.4, 3.1, 0.5};
    assertThat(MutualInformationDC.miRaw(d1, c1, k, base)).isCloseTo(0.5889, byLessThan(DELTA));

    double[] c2 = new double[] {1.0, 1.0, 2.3, 2.4, 3.1, 3.2};
    assertThat(MutualInformationDC.miRaw(d1, c2, k, base)).isCloseTo(1.2833, byLessThan(DELTA));

    double[] c3 = new double[] {1.0, 1.0, 2.3, 3.4, 3.1, 0.5};
    assertThat(MutualInformationDC.miRaw(d1, c3, k, base)).isCloseTo(0.2972, byLessThan(DELTA));

    double[] c4 = new double[] {1.0, 1.0, 2.3, 3.4, 3.1, 3.2};
    assertThat(MutualInformationDC.miRaw(d1, c4, k, base)).isCloseTo(0.7833, byLessThan(DELTA));

    double[] c5 = new double[] {3.0, 1.0, 2.3, 3.4, 3.1, 3.2};
    assertThat(MutualInformationDC.miRaw(d1, c5, k, base)).isCloseTo(-0.0083, byLessThan(DELTA));
    assertThat(MutualInformationDC.mi(d1, c5, k)).isZero();

    //
    // Case 2, different discrete variable
    //
    int[] d2 = new int[] {1, 2, 2, 2, 3, 3};

    double[] c6 = new double[] {3.0, 100.0, 2.3, 3.4, 3.1, 3.2};
    assertThat(MutualInformationDC.miRaw(d2, c6, k, base)).isCloseTo(0.1111, byLessThan(DELTA));

    double[] c7 = new double[] {3.0, 100.0, 2.3, 3.4, 3.1, 103.2};
    assertThat(MutualInformationDC.miRaw(d2, c7, k, base)).isCloseTo(-0.2361, byLessThan(DELTA));
    assertThat(MutualInformationDC.mi(d2, c7, k)).isZero();

    double[] c8 = new double[] {3.0, 100.0, 2.3, 3.4, Double.NaN, 103.2};
    assertThat(MutualInformationDC.miRaw(d2, c8, k, base)).isCloseTo(0.5139, byLessThan(DELTA));

    //
    // Case 3, with NaNs, +Inf, and -Inf.
    //
    int[] d3 = new int[] {0, 1, 2, 2, 2, 3, 4, 5};

    double[] c9 = new double[] {3.0, 100.0, -2.3, -103.4, 0, 0, 0, 0};
    assertThat(MutualInformationDC.miRaw(d3, c9, k, base)).isCloseTo(-0.1987, byLessThan(DELTA));
    assertThat(MutualInformationDC.mi(d3, c9, k)).isZero();

    double[] c10 = new double[] {0, 0, 0, 1e99, 0, 0, 0, 0};
    assertThat(MutualInformationDC.miRaw(d3, c10, k, base)).isCloseTo(-0.4008, byLessThan(DELTA));
    assertThat(MutualInformationDC.mi(d3, c10, k)).isZero();

    double[] c11 = new double[] {0, 0, 0, 1e99, 0, Double.POSITIVE_INFINITY, 0, 0};
    assertThat(MutualInformationDC.miRaw(d3, c11, k, base)).isCloseTo(-0.3383, byLessThan(DELTA));
    assertThat(MutualInformationDC.mi(d3, c11, k)).isZero();

    double[] c12 = new double[] {0, 0, 0, 1e99, 0, Double.POSITIVE_INFINITY, Double.NaN, 0};
    assertThat(MutualInformationDC.miRaw(d3, c12, k, base)).isCloseTo(-0.2633, byLessThan(DELTA));
    assertThat(MutualInformationDC.mi(d3, c12, k)).isZero();

    double[] c13 = new double[] {0, -1, 0, 1e99, 0, Double.POSITIVE_INFINITY, Double.NaN, 0};
    assertThat(MutualInformationDC.miRaw(d3, c13, k, base)).isCloseTo(-0.2321, byLessThan(DELTA));
    assertThat(MutualInformationDC.mi(d3, c13, k)).isZero();

    double[] c14 =
        new double[] {
          Double.NEGATIVE_INFINITY, -1, 0, 1e99, 0, Double.POSITIVE_INFINITY, Double.NaN, 0
        };
    assertThat(MutualInformationDC.miRaw(d3, c14, k, base)).isCloseTo(-0.1279, byLessThan(DELTA));
    assertThat(MutualInformationDC.mi(d3, c14, k)).isZero();
  }

  /**
   * This tests compares the current output with previous saved outputs stored in a snapshot file.
   * This is intended to capture any unintended changes to the output. If the changes are intended,
   * the snapshot file must be deleted and the snapshot file will be regenerated on the first run of
   * this test. The snapshot must then be added to the source code versioning system (Git).
   */
  @Test
  public void testSnapshotRegression() throws IOException {
    final int runs = 1000;
    final double base = Math.E;
    int seed = 123;
    Random rng = new Random(seed);
    double[] output = new double[runs];
    for (int run = 0; run < runs; run++) {
      int n = rng.nextInt(10000);
      int k = 3 + rng.nextInt(5); // k in range [3,7]
      int[] d = new int[n];
      double[] c = new double[n];
      double p = rng.nextDouble();
      int categories = 2 + rng.nextInt(1000);
      for (int i = 0; i < n; i++) {
        d[i] = rng.nextInt() % categories;
        c[i] = rng.nextDouble() + p * d[i];
      }
      output[run] = MutualInformationDC.miRaw(d, c, k, base);
    }

    String filename = "mi-snapshot-test-data_seed-" + seed + ".txt";
    URL snapshotTestFile = getClass().getClassLoader().getResource(filename);
    if (snapshotTestFile == null) {
      // if the snapshot does not exist, we regenerate it at the resource folder.
      // the next run will use the snapshot.
      Path testResourcesPath = Paths.get("src/test/resources/", filename);
      generateSnapshotFile(output, testResourcesPath);
      String failureMessage =
          String.format(
              "Snapshot file not found. Generated snapshot at %s. "
                  + "This file must be committed to Git to keep output state.",
              testResourcesPath);
      fail(failureMessage);
    }

    // read expected output from snapshot file
    Path buildResourcePath = Paths.get(snapshotTestFile.getPath());
    double[] expected =
        Files.readAllLines(buildResourcePath).stream()
            .mapToDouble(s -> Double.parseDouble(s))
            .toArray();

    // ensure that snapshot data has same size as this run
    assertThat(output.length).isEqualTo(expected.length);
    // ensure same output from previous runs
    for (int i = 0; i < expected.length; i++) {
      assertThat(output[i]).isCloseTo(expected[i], byLessThan(0.000001));
    }
  }

  private void generateSnapshotFile(double[] output, Path testResourcesPath)
      throws FileNotFoundException {
    System.out.println("Generating new MI test file at: " + testResourcesPath.toAbsolutePath());
    try (PrintWriter f = new PrintWriter(testResourcesPath.toFile())) {
      for (double mi : output) {
        f.println(mi);
      }
    }
  }
}
