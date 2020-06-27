package sketches.correlation.estimators;

import java.util.Arrays;
import java.util.Random;
import org.junit.jupiter.api.Test;
import sketches.correlation.estimators.BootstrapedPearson.CI;
import sketches.statistics.Stats;

public class BootstrapedCorrelationTest {

  @Test
  public void shouldComputeUsingBootstrapingEstimator() {
    Random r = new Random();
    //    double[] x = {0,0,0,0,1,1,0,0,0,0,0,0};
//    double[] y = {0, 0, 0, 0, 0, 1, 0, 1, 0, 1, 0, 0};
//    double[] x = {1, 1, 1, 1, 1, 2, 1, 1, 1, 1, 1, 1};
//    final int n = x.length;

    int runs = 1000;
    long[] runningTimes = new long[runs];
    CI[] estimates = new CI[runs];
    for (int i = 0; i < runs; i++) {

      final int n = 25 + r.nextInt(512);
      double[] y = new double[n];
      double[] x = new double[n];
      for (int j = 0; j < n; j++) {
        x[j] = r.nextGaussian()*(1_000_000);
//      y[j] = r.nextGaussian();
//      y[j] = x[j] + (r.nextGaussian() > r.nextGaussian() ? 3 : Math.log(1-r.nextDouble())/-1); // r.nextGaussian());
//      y[j] = x[j]*(-10)*r.nextGaussian();
        y[j] = Math.log(1 - r.nextDouble()) / (-1);
      }

      long t0 = System.nanoTime();
      estimates[i] = BootstrapedPearson.coefficient(x, y);
      long t1 = System.nanoTime();
      runningTimes[i] = (t1 - t0);
    }
    Arrays.sort(runningTimes);

    double alpha = 0.05;
    double percentile = alpha / 2.0;
    int idxLb = (int) Math.ceil((percentile) * runs);
    int idxUb = (int) Math.ceil((1. - percentile) * runs);
    long lb = runningTimes[idxLb - 1];
    long ub = runningTimes[idxUb - 1];

    System.out.printf("mean time: %.3f ms\n", Stats.mean(runningTimes) / 1_000_000.);
    System.out.printf("lb: %.3f\n", lb / 1_000_000.);
    System.out.printf("ub: %.3f\n", ub / 1_000_000.);

    System.out.println(Arrays.toString(estimates));
  }

}
