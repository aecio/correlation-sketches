package corrsketches.statistics;

import static corrsketches.statistics.DifferentialEntropyMixed.kthNearestNonZero;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

import corrsketches.util.RandomArrays;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;

public class DifferentialEntropyMixedTests {

  final double DELTA = 0.00000001;

  @Test
  public void testDifferentialEntropyKL() {

    double[] a = new double[] {1, 2, 4, 8, 16};
    assertThat(DifferentialEntropyMixed.entropy(a)).isEqualTo(3.218266805508045, within(DELTA));

    double[] b = new double[] {1, 2, 1, 2, 1.2};
    assertThat(DifferentialEntropyMixed.entropy(b)).isCloseTo(1.2318518036304367, within(DELTA));

    double[] c =
        new double[] {
          -0.59152691,
          -0.21027888,
          1.40407995,
          -0.53021491,
          0.58272939,
          -0.23601182,
          -1.19971974,
          -1.50147482,
          0.25556115,
          -0.06472547,
          -0.56735615,
          -0.38815229,
          -1.10666078,
          -0.26985764,
          0.1365975
        };
    assertThat(DifferentialEntropyMixed.entropy(c)).isCloseTo(1.2231987815353995, within(DELTA));
  }

  @Test
  @RepeatedTest(10)
  public void testKnownDistributions() {
    // This test compares the estimated entropy with the exact entropy computed using the exact
    // formulas for the given probability distributions. See formula at:
    // https://en.wikipedia.org/wiki/Differential_entropy
    double expectedEntropy;
    double[] x;
    int N = 10 * 1024;
    double allowedError = 0.1;

    // entropy for standard normal distribution N(0, 1): ln(var*sqrt(2*pi*e))
    x = RandomArrays.randDoubleStdNormal(N); // mean: 0 var: 1
    expectedEntropy = Math.log(1 * Math.sqrt(2 * Math.PI * Math.E));
    assertThat(DifferentialEntropyMixed.entropy(x, 3))
        .isCloseTo(expectedEntropy, within(allowedError));

    // entropy for the exponential distribution with parameter lambda: 1 - ln(lambda)
    final double lambda = 2;
    x = RandomArrays.randDoubleExponential(N, lambda);
    expectedEntropy = 1 - Math.log(lambda);
    assertThat(DifferentialEntropyMixed.entropy(x, 3))
        .isCloseTo(expectedEntropy, within(allowedError));

    // entropy for uniform distribution in range [a, b] is = ln(b-a)
    x = RandomArrays.randDoubleUniform(N); // [a, b]=[0, 1]
    expectedEntropy = Math.log(1); //  ln(b-a) = ln(1)
    assertThat(DifferentialEntropyMixed.entropy(x, 3))
        .isCloseTo(expectedEntropy, within(allowedError));
  }

  @Test
  //  @RepeatedTest(10)
  public void testKnownMixtureOfDistributions() {
    // This test compares the estimated entropy with the exact entropy computed using the exact
    // formulas for the given probability distributions. See formula at:
    // https://en.wikipedia.org/wiki/Differential_entropy
    double expectedEntropy;
    double[] x;
    int N = 10 * 1024;
    //    int N = 4;
    double allowedError = 0.1;

    //    // entropy for standard normal distribution N(0, 1): ln(var*sqrt(2*pi*e))
    //    x = RandomArrays.randDoubleStdNormal(N); // mean: 0 var: 1
    //    expectedEntropy = Math.log(1 * Math.sqrt(2 * Math.PI * Math.E));
    //    assertThat(DifferentialEntropyMixed.entropy(x, 3)).isCloseTo(expectedEntropy,
    // within(allowedError));

    // entropy for the exponential distribution with parameter lambda: 1 - ln(lambda)
    //    final double lambda = 2;
    //    x = RandomArrays.randDoubleExponential(N, lambda);
    //    expectedEntropy = 1 - Math.log(lambda);
    //    assertThat(DifferentialEntropyMixed.entropy(x, 3)).isCloseTo(expectedEntropy,
    // within(allowedError));

    // entropy for uniform distribution in range [a, b] is = ln(b-a)
    x = RandomArrays.randDoubleRademacher(N); // [a, b]=[0, 1]
    int[] z = new int[x.length];
    for (int i = 0; i < z.length; i++) {
      z[i] = (int) x[i];
    }
    expectedEntropy = Math.log(2); //  ln(b-a) = ln(1)
    assertThat(Entropy.entropy(z)).isCloseTo(expectedEntropy, within(allowedError));

    //    Random rng = new Random(7);
    //    for (int i = 0; i < x.length; i++) {
    //      x[i] += rng.nextGaussian() * 1e-15;
    //    }
    //    assertThat(DifferentialEntropyMixed.entropy(x, 3)).isCloseTo(expectedEntropy,
    // within(allowedError));

    double[] q = new double[2 * N];
    System.arraycopy(RandomArrays.randDoubleUniform(N + N), 0, q, 0, N + N);
    System.arraycopy(RandomArrays.randDoubleRademacher(N), 0, q, N, N);
    //    System.out.println("q = " + Arrays.toString(q));
    expectedEntropy =
        (Math.log(1)
            + // uniform [0,1]: ln(b-a) = ln(1)
            0 // Math.log(2) // discrete n=2: ln(n) = ln(2)
        );
    assertThat(DifferentialEntropyMixed.entropy(q, 3))
        .isCloseTo(expectedEntropy, within(allowedError));

    x = RandomArrays.randDoubleUniform(N); // [a, b]=[0, 1]
    z = new int[x.length];
    for (int i = 0; i < z.length; i++) {
      if (x[i] < 1d / 4d) {
        z[i] = -1;
      } else if (x[i] < 2d / 4d) {
        z[i] = 0;
      } else if (x[i] < 3d / 4d) {
        z[i] = 2;
      } else {
        z[i] = 1;
      }
    }
    expectedEntropy = Math.log(4); //  ln(b-a) = ln(1)
    assertThat(Entropy.entropy(z)).isCloseTo(expectedEntropy, within(allowedError));
    assertThat(DifferentialEntropyMixed.entropy(x, 3))
        .isCloseTo(expectedEntropy, within(allowedError));
  }

  @Test
  public void testNearestK() {
    double[] x = new double[] {1, 2, 4, 8, 16};

    // distances for targetIdx=[0]: {0, 1, 3, 7, 15}
    int targetIdx = 0;
    assertThat(kthNearestNonZero(x, targetIdx, 1).kthNearest).isEqualTo(2);
    assertThat(kthNearestNonZero(x, targetIdx, 2).kthNearest).isEqualTo(4);
    assertThat(kthNearestNonZero(x, targetIdx, 3).kthNearest).isEqualTo(8);
    assertThat(kthNearestNonZero(x, targetIdx, 4).kthNearest).isEqualTo(16);
    assertThat(kthNearestNonZero(x, targetIdx, 5).kthNearest).isEqualTo(16);
    assertThat(kthNearestNonZero(x, targetIdx, 6).kthNearest).isEqualTo(16);

    assertThat(kthNearestNonZero(x, targetIdx, 1).distance).isEqualTo(1);
    assertThat(kthNearestNonZero(x, targetIdx, 2).distance).isEqualTo(3);
    assertThat(kthNearestNonZero(x, targetIdx, 3).distance).isEqualTo(7);
    assertThat(kthNearestNonZero(x, targetIdx, 4).distance).isEqualTo(15);
    assertThat(kthNearestNonZero(x, targetIdx, 5).distance).isEqualTo(15);
    assertThat(kthNearestNonZero(x, targetIdx, 6).distance).isEqualTo(15);

    // distances for targetIdx=[2]: {3, 2, 0, 4, 12}
    targetIdx = 2;
    assertThat(kthNearestNonZero(x, targetIdx, 1).kthNearest).isEqualTo(2);
    assertThat(kthNearestNonZero(x, targetIdx, 2).kthNearest).isEqualTo(1);
    assertThat(kthNearestNonZero(x, targetIdx, 3).kthNearest).isEqualTo(8);
    assertThat(kthNearestNonZero(x, targetIdx, 4).kthNearest).isEqualTo(16);
    assertThat(kthNearestNonZero(x, targetIdx, 5).kthNearest).isEqualTo(16);
    assertThat(kthNearestNonZero(x, targetIdx, 6).kthNearest).isEqualTo(16);

    assertThat(kthNearestNonZero(x, targetIdx, 1).distance).isEqualTo(2);
    assertThat(kthNearestNonZero(x, targetIdx, 2).distance).isEqualTo(3);
    assertThat(kthNearestNonZero(x, targetIdx, 3).distance).isEqualTo(4);
    assertThat(kthNearestNonZero(x, targetIdx, 4).distance).isEqualTo(12);
    assertThat(kthNearestNonZero(x, targetIdx, 5).distance).isEqualTo(12);
    assertThat(kthNearestNonZero(x, targetIdx, 6).distance).isEqualTo(12);
  }

  @Test
  public void testNearestKMixed() {
    double[] x = new double[] {0, 0, 0, 0, 1, 2, 4, 8, 16};

    // distances to x[0]:     {0, 0, 0, 0, 1, 2, 4, 8, 16}
    int targetIdx = 0;
    assertThat(kthNearestNonZero(x, targetIdx, 1).kthNearest).isEqualTo(1);
    assertThat(kthNearestNonZero(x, targetIdx, 2).kthNearest).isEqualTo(1);
    assertThat(kthNearestNonZero(x, targetIdx, 3).kthNearest).isEqualTo(1);
    assertThat(kthNearestNonZero(x, targetIdx, 4).kthNearest).isEqualTo(1);
    assertThat(kthNearestNonZero(x, targetIdx, 5).kthNearest).isEqualTo(2);
    assertThat(kthNearestNonZero(x, targetIdx, 6).kthNearest).isEqualTo(4);

    assertThat(kthNearestNonZero(x, targetIdx, 1).kthNearest).isEqualTo(1);
    assertThat(kthNearestNonZero(x, targetIdx, 2).kthNearest).isEqualTo(1);
    assertThat(kthNearestNonZero(x, targetIdx, 3).kthNearest).isEqualTo(1);
    assertThat(kthNearestNonZero(x, targetIdx, 4).kthNearest).isEqualTo(1);
    assertThat(kthNearestNonZero(x, targetIdx, 5).kthNearest).isEqualTo(2);
    assertThat(kthNearestNonZero(x, targetIdx, 6).kthNearest).isEqualTo(4);

    assertThat(kthNearestNonZero(x, targetIdx, 1).distance).isEqualTo(1);
    assertThat(kthNearestNonZero(x, targetIdx, 2).distance).isEqualTo(1);
    assertThat(kthNearestNonZero(x, targetIdx, 3).distance).isEqualTo(1);
    assertThat(kthNearestNonZero(x, targetIdx, 4).distance).isEqualTo(1);
    assertThat(kthNearestNonZero(x, targetIdx, 5).distance).isEqualTo(2);
    assertThat(kthNearestNonZero(x, targetIdx, 6).distance).isEqualTo(4);

    assertThat(kthNearestNonZero(x, targetIdx, 1).k).isEqualTo(4);
    assertThat(kthNearestNonZero(x, targetIdx, 2).k).isEqualTo(4);
    assertThat(kthNearestNonZero(x, targetIdx, 3).k).isEqualTo(4);
    assertThat(kthNearestNonZero(x, targetIdx, 4).k).isEqualTo(4);
    assertThat(kthNearestNonZero(x, targetIdx, 5).k).isEqualTo(5);
    assertThat(kthNearestNonZero(x, targetIdx, 6).k).isEqualTo(6);
  }
}
