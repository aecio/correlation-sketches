package corrsketches.statistics;

import smile.stat.distribution.GaussianDistribution;

public class Variance {

  private static final GaussianDistribution GAUSSIAN = GaussianDistribution.getInstance();

  /** Computes the unbiased sample variance of the given vector. */
  public static double var(double[] x) {
    if (x.length == 0) {
      return 0.0;
    }
    double sumx = 0.0, sumx2 = 0;
    for (double xi : x) {
      sumx += xi;
      sumx2 += xi * xi;
    }
    final double n = x.length - 1;
    return sumx2 / n - (sumx / x.length) * (sumx / n);
  }

  /**
   * Implements the augmented large-sample confidence interval from the paper "Estimating kurtosis
   * and confidence intervals for the variance under non-normality", Journal of Statistical
   * Computation and Simulation, 2013.
   */
  public static CI alsci(double[] x, double alpha) {
    final int n = x.length;

    final double k = Kurtosis.kc(x, 5);
    final double k2n = (k + (2. * n / (double) (n - 1)));

    final double s2 = var(x);
    final double z = GAUSSIAN.quantile(1. - (alpha / 2));
    final double varLogS2 = (1. / (double) n) * k2n * (1. + ((1. / 2. * n) * k2n));
    final double a = (k + ((2. * n) / (n - 1.))) / (2. * n);

    final double lb = s2 * Math.exp(-z * varLogS2 + a);
    final double ub = s2 * Math.exp(+z * varLogS2 + a);

    return new CI(s2, lb, ub);
  }

  public static class CI {

    public final double var;
    public final double lb;
    public final double ub;

    public CI(double var, double lb, double ub) {
      this.var = var;
      this.lb = lb;
      this.ub = ub;
    }

    @Override
    public String toString() {
      return String.format("CI{var=%.3f lb=%.3f ub=%.3f width=%.3f}", var, lb, ub, ub - lb);
    }
  }
}
