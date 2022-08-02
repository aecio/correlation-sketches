package corrsketches.statistics;

import static corrsketches.statistics.DifferentialEntropy.distanceToKthNearest;
import static corrsketches.statistics.DifferentialEntropy.kthNearest;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

import corrsketches.util.RandomArrays;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;

public class DifferentialEntropyTests {

  final double DELTA = 0.00000001;

  @Test
  public void testDifferentialEntropyKL() {

    double[] a = new double[] {1, 2, 4, 8, 16};
    assertThat(DifferentialEntropy.entropy(a)).isEqualTo(3.218266805508045, within(DELTA));

    double[] b = new double[] {1, 2, 1, 2, 1.2};
    assertThat(DifferentialEntropy.entropy(b)).isCloseTo(1.2318518036304367, within(DELTA));

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
    assertThat(DifferentialEntropy.entropy(c)).isCloseTo(1.2231987815353995, within(DELTA));
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
    assertThat(DifferentialEntropy.entropy(x, 3)).isCloseTo(expectedEntropy, within(allowedError));

    // entropy for the exponential distribution with parameter lambda: 1 - ln(lambda)
    final double lambda = 2;
    x = RandomArrays.randDoubleExponential(N, lambda);
    expectedEntropy = 1 - Math.log(lambda);
    assertThat(DifferentialEntropy.entropy(x, 3)).isCloseTo(expectedEntropy, within(allowedError));

    // entropy for uniform distribution in range [a, b] is = ln(b-a)
    x = RandomArrays.randDoubleUniform(N); // [a, b]=[0, 1]
    expectedEntropy = Math.log(1); //  ln(b-a) = ln(1)
    assertThat(DifferentialEntropy.entropy(x, 3)).isCloseTo(expectedEntropy, within(allowedError));
  }

  @Test
  public void testNearestK() {
    double[] x = new double[] {1, 2, 4, 8, 16};

    // distances for targetIdx=[2]: {0, 1, 3, 7, 15}
    int targetIdx = 0;
    assertThat(kthNearest(x, targetIdx, 1)).isEqualTo(2);
    assertThat(kthNearest(x, targetIdx, 2)).isEqualTo(4);
    assertThat(kthNearest(x, targetIdx, 3)).isEqualTo(8);
    assertThat(kthNearest(x, targetIdx, 4)).isEqualTo(16);
    assertThat(kthNearest(x, targetIdx, 5)).isEqualTo(16);
    assertThat(kthNearest(x, targetIdx, 6)).isEqualTo(16);

    assertThat(distanceToKthNearest(x, targetIdx, 1)).isEqualTo(1);
    assertThat(distanceToKthNearest(x, targetIdx, 2)).isEqualTo(3);
    assertThat(distanceToKthNearest(x, targetIdx, 3)).isEqualTo(7);
    assertThat(distanceToKthNearest(x, targetIdx, 4)).isEqualTo(15);
    assertThat(distanceToKthNearest(x, targetIdx, 5)).isEqualTo(15);
    assertThat(distanceToKthNearest(x, targetIdx, 6)).isEqualTo(15);

    // distances for targetIdx=[2]: {3, 2, 0, 4, 12}
    targetIdx = 2;
    assertThat(kthNearest(x, targetIdx, 1)).isEqualTo(2);
    assertThat(kthNearest(x, targetIdx, 2)).isEqualTo(1);
    assertThat(kthNearest(x, targetIdx, 3)).isEqualTo(8);
    assertThat(kthNearest(x, targetIdx, 4)).isEqualTo(16);
    assertThat(kthNearest(x, targetIdx, 5)).isEqualTo(16);
    assertThat(kthNearest(x, targetIdx, 6)).isEqualTo(16);

    assertThat(distanceToKthNearest(x, targetIdx, 1)).isEqualTo(2);
    assertThat(distanceToKthNearest(x, targetIdx, 2)).isEqualTo(3);
    assertThat(distanceToKthNearest(x, targetIdx, 3)).isEqualTo(4);
    assertThat(distanceToKthNearest(x, targetIdx, 4)).isEqualTo(12);
    assertThat(distanceToKthNearest(x, targetIdx, 5)).isEqualTo(12);
    assertThat(distanceToKthNearest(x, targetIdx, 6)).isEqualTo(12);
  }
}
