package sketches.statistics;

import smile.stat.distribution.GaussianDistribution;

public class Stats {

  public static final GaussianDistribution NORMAL = new GaussianDistribution(0, 1);

  /**
   * Computes the mean of the given input vector.
   *
   * @return sum(x)/n
   */
  public static double mean(long[] x) {
    long sum = 0;
    for (int i = 0; i < x.length; i++) {
      sum += x[i];
    }
    return sum / (double) x.length;
  }

  /**
   * Computes the mean of the given input vector.
   *
   * @return sum(x)/n
   */
  public static double mean(double[] x) {
    double sum = 0.0;
    for (int i = 0; i < x.length; i++) {
      sum += x[i];
    }
    return sum / x.length;
  }
}
