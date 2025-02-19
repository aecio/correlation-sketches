package corrsketches.statistics;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

import corrsketches.util.RandomArrays;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;

public class DifferentialEntropyMixedTests {

  final double DELTA = 0.00000001;

  @Test
  public void testDifferentialEntropyKL() {

    double[] x = new double[] {1, 2, 4, 8, 16};
    assertThat(DifferentialEntropyMixed.entropy(x)).isEqualTo(3.218266805508045, within(DELTA));

    x = new double[] {1, 2, 1, 2, 1.2};
    assertThat(DifferentialEntropyMixed.entropy(x)).isCloseTo(1.2318518036304367, within(DELTA));

    x =
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
    assertThat(DifferentialEntropyMixed.entropy(x)).isCloseTo(1.2231987815353995, within(DELTA));

    x = new double[] {1, 1};
    assertThat(DifferentialEntropyMixed.entropy(x))
        .isEqualTo(Double.NEGATIVE_INFINITY, within(DELTA));

    x = new double[] {1, 1, 1, 1, 1, 1, 1};
    assertThat(DifferentialEntropyMixed.entropy(x))
        .isEqualTo(Double.NEGATIVE_INFINITY, within(DELTA));
  }

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

  //  @Test
  //  //  @RepeatedTest(10)
  //  public void testKnownMixtureOfDistributions() {
  //    // This test compares the estimated entropy with the exact entropy computed using the exact
  //    // formulas for the given probability distributions. See formula at:
  //    // https://en.wikipedia.org/wiki/Differential_entropy
  //    double expectedEntropy;
  //    double[] x;
  //    int N = 10 * 1024;
  //    //    int N = 4;
  //    double allowedError = 0.1;
  //
  //    //    // entropy for standard normal distribution N(0, 1): ln(var*sqrt(2*pi*e))
  //    //    x = RandomArrays.randDoubleStdNormal(N); // mean: 0 var: 1
  //    //    expectedEntropy = Math.log(1 * Math.sqrt(2 * Math.PI * Math.E));
  //    //    assertThat(DifferentialEntropyMixed.entropy(x, 3)).isCloseTo(expectedEntropy,
  //    // within(allowedError));
  //
  //    // entropy for the exponential distribution with parameter lambda: 1 - ln(lambda)
  //    //    final double lambda = 2;
  //    //    x = RandomArrays.randDoubleExponential(N, lambda);
  //    //    expectedEntropy = 1 - Math.log(lambda);
  //    //    assertThat(DifferentialEntropyMixed.entropy(x, 3)).isCloseTo(expectedEntropy,
  //    // within(allowedError));
  //
  //    // entropy for uniform distribution in range [a, b] is = ln(b-a)
  //    x = RandomArrays.randDoubleRademacher(N); // [a, b]=[0, 1]
  //    int[] z = new int[x.length];
  //    for (int i = 0; i < z.length; i++) {
  //      z[i] = (int) x[i];
  //    }
  //    expectedEntropy = Math.log(2); //  ln(b-a) = ln(1)
  //    assertThat(Entropy.entropy(z)).isCloseTo(expectedEntropy, within(allowedError));
  //
  //    //    Random rng = new Random(7);
  //    //    for (int i = 0; i < x.length; i++) {
  //    //      x[i] += rng.nextGaussian() * 1e-15;
  //    //    }
  //    //    assertThat(DifferentialEntropyMixed.entropy(x, 3)).isCloseTo(expectedEntropy,
  //    // within(allowedError));
  //
  //    double[] q = new double[2 * N];
  //    System.arraycopy(RandomArrays.randDoubleUniform(N + N), 0, q, 0, N + N);
  //    System.arraycopy(RandomArrays.randDoubleRademacher(N), 0, q, N, N);
  //    //    System.out.println("q = " + Arrays.toString(q));
  //    expectedEntropy =
  //        (Math.log(1)
  //            + // uniform [0,1]: ln(b-a) = ln(1)
  //            0 // Math.log(2) // discrete n=2: ln(n) = ln(2)
  //        );
  //    assertThat(DifferentialEntropyMixed.entropy(q, 3))
  //        .isCloseTo(expectedEntropy, within(allowedError));
  //
  //    x = RandomArrays.randDoubleUniform(N); // [a, b]=[0, 1]
  //    z = new int[x.length];
  //    for (int i = 0; i < z.length; i++) {
  //      if (x[i] < 1d / 4d) {
  //        z[i] = -1;
  //      } else if (x[i] < 2d / 4d) {
  //        z[i] = 0;
  //      } else if (x[i] < 3d / 4d) {
  //        z[i] = 2;
  //      } else {
  //        z[i] = 1;
  //      }
  //    }
  //    expectedEntropy = Math.log(4); //  ln(b-a) = ln(1)
  //    assertThat(Entropy.entropy(z)).isCloseTo(expectedEntropy, within(allowedError));
  //    assertThat(DifferentialEntropyMixed.entropy(x, 3))
  //        .isCloseTo(expectedEntropy, within(allowedError));
  //  }
}
