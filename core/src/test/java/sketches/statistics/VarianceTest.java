package sketches.statistics;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Random;
import org.junit.jupiter.api.Test;
import sketches.statistics.Variance.CI;
import smile.math.Math;

public class VarianceTest {

  @Test
  public void testSampleVarianceEstimator() {
    double[] x;

    x = new double[]{1, 2, 3, 4};
    assertEquals(1.666667, Variance.var(x), 0.00001);
  }

  @Test
  public void testSampleVarianceConfidenceInterval() {
    Random r = new Random();
//    double[] x = new double[]{0, 3, 4, 1, 0, 10, 0, 2, 1, 4, 2, 0, 3, 2, 8, 2, 5, 50, 80, 100};
    int runs = 1000;
    int[] coverage = new int[runs];
    for (int i = 0; i < runs; i++) {
      // create population
      int N = 1000;
      double[] x = new double[N];
      if (r.nextDouble() > 1.0) {
        for (int j = 0; j < N; j++) {
          x[j] = r.nextGaussian();
        }
      } else {
        for (int j = 0; j < N; j++) {
          x[j] = Math.log(1 - r.nextDouble()) / -1;
        }
      }
      // random sample
      int[] permutation = Math.permutate(N);
//      int n = 5 + r.nextInt(30);
      int n = 200;
      double[] sample = new double[n];
      for (int j = 0; j < n; j++) {
        sample[j] = x[permutation[j]];
      }

      double varx = Variance.var(x);
      double s2 = Variance.var(sample);

      double alpha = .95;
      CI ci = Variance.alsci(sample, alpha);
      if (i % 25 == 0) {
        System.out
            .printf("s2 = %.2f sig2 = %.1f delta = %.2f k_est = %3.2f k_pop = %3.2f ci = %s\n", s2,
                varx, Math.abs(varx - s2), Kurtosis.G2(sample), Kurtosis.G2(x), ci);
      }

      coverage[i] = (varx < ci.ub && varx > ci.lb) ? 1 : 0;
    }
    double p = Math.mean(coverage);
    System.out.printf("CI coverage: %.3f\n", p);
    assertTrue(p > 0.6);

  }

}
