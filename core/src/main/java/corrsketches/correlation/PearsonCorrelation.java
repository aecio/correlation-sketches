package corrsketches.correlation;

import corrsketches.correlation.Correlation.Estimate;
import smile.stat.distribution.GaussianDistribution;
import smile.stat.distribution.TDistribution;

/** Implements Pearson's product-moment correlation coefficient. */
public class PearsonCorrelation {

  public static final double TINY = 1.0e-25;

  private static final GaussianDistribution NORMAL = new GaussianDistribution(0, 1);
  private static final ConfidenceInterval NULL_CI = new ConfidenceInterval(Double.NaN, Double.NaN);

  /**
   * Computes the Pearson product-moment correlation coefficient for two vectors. When the vector
   * covariances are zero (or close to zero), i.e., the series are constant, this implementation
   * returns Double.NaN.
   */
  public static Estimate estimate(double[] x, double[] y) {
    double r = coefficient(x, y);
    return new Estimate(r, x.length);
  }

  /**
   * Computes the Pearson product-moment correlation coefficient for two vectors. When the vector
   * covariances are zero (or close to zero), i.e., the series are constant, this implementation
   * returns Double.NaN.
   */
  public static double coefficient(double[] x, double[] y) {

    int n = x.length;
    double syy = 0.0, sxy = 0.0, sxx = 0.0, ay = 0.0, ax = 0.0;

    for (int i = 0; i < n; i++) {
      ax += x[i];
      ay += y[i];
    }

    ax /= n;
    ay /= n;

    for (int i = 0; i < n; i++) {
      double xt = x[i] - ax;
      double yt = y[i] - ay;
      sxx += xt * xt;
      syy += yt * yt;
      sxy += xt * yt;
    }

    if (!(sxx > TINY && syy > TINY)) {
      return Double.NaN;
    }
    return sxy / Math.sqrt(sxx * syy);
  }

  /**
   * Given a Pearson correlation coefficient and the sample size, this function computes the p-value
   * of a two-tailed t-test against the null hypothesis (correlation equal to zero).
   *
   * @param coefficient Pearson correlation coefficient
   * @param sampleSize sample size used to calculate the coefficient
   * @return p-value of the t-test
   */
  public static double pValueTwoTailed(double coefficient, int sampleSize) {
    return 2 * pValueOneTailed(coefficient, sampleSize);
  }

  /**
   * Given a Pearson correlation coefficient and the sample size, this function computes the p-value
   * of a one-tailed t-test against the null hypothesis (correlation equal to zero).
   *
   * @param coefficient Pearson correlation coefficient
   * @param sampleSize sample size used to calculate the coefficient
   * @return p-value of the t-test
   */
  public static double pValueOneTailed(double coefficient, int sampleSize) {
    int degreesOfFreedom = sampleSize - 2;
    TDistribution tDistribution = new TDistribution(degreesOfFreedom);
    double tScore = tScore(coefficient, sampleSize);
    double probability = tDistribution.cdf(tScore);
    return 1. - probability;
  }

  /**
   * Performs a significance t-test (against the null hypothesis that the Pearson's coefficient r is
   * equal to zero.
   *
   * @param coefficient The Pearson coefficient r
   * @param sampleSize The sample sized used to calculate r
   * @param significance The level of significance (alpha) of the test
   * @return true if it is statistically significant, false otherwise
   */
  public static boolean isSignificant(double coefficient, int sampleSize, double significance) {
    int degreesOfFreedom = sampleSize - 2;
    TDistribution tDistribution = new TDistribution(degreesOfFreedom);
    double tScore = tScore(coefficient, sampleSize);
    double criticalT = tDistribution.quantile2tiled(1. - significance);
    return tScore >= criticalT;
  }

  private static double tScore(double coefficient, int sampleSize) {
    final double r = Math.abs(coefficient); // TODO: is this correct?
    final int n = sampleSize;
    return r * Math.sqrt((n - 2) / (1 - r * r));
  }

  @SuppressWarnings("unused")
  private static double criticalR(int degreesOfFreedom, double criticalT) {
    double criticalT2 = criticalT * criticalT;
    return Math.sqrt(criticalT2 / (criticalT2 + degreesOfFreedom));
  }

  /**
   * Computes the confidence intervals for the given Pearson's correlation coefficient, sample size
   * n, and confidence level (in percentage).
   *
   * @param r the correlation coefficient
   * @param n the sample size from which the coefficient was computed
   * @param confidence the desired confidence in percentage (e.g., 0.95).
   * @return an object containing the interval (lower and upper bounds)
   */
  public static ConfidenceInterval confidenceInterval(double r, int n, double confidence) {
    if (n < 4) {
      return NULL_CI;
    }
    double alpha = (1. - confidence) / 2;

    double z = NORMAL.quantile(1. - alpha);
    double zstderr = 1. / Math.sqrt(n - 3);
    double interval = z * zstderr;

    r = r >= +1. ? (+1. - 1.0e-15) : r; // prevent Infinity error
    r = r <= -1. ? (-1. + 1.0e-15) : r; // prevent Infinity error
    double zp = rtoz(r);
    double ubz = zp + interval;
    double lbz = zp - interval;

    double ub = ztor(ubz);
    double lb = ztor(lbz);

    return new ConfidenceInterval(lb, ub);
  }

  /**
   * Computes Fisher's transformation function from r to z.
   *
   * @param r the sample Pearson's correlation coefficient
   * @return z, the Fisher's z-transformation
   */
  private static double rtoz(double r) {
    return 0.5 * Math.log((1 + r) / (1 - r));
  }

  /**
   * Computes Fisher's transformation function from z to r.
   *
   * @param z, the Fisher's z-transformation
   * @return r, the sample Pearson's correlation coefficient
   */
  private static double ztor(double z) {
    double exp2z = Math.exp(2 * z);
    return ((exp2z - 1) / (exp2z + 1));
  }

  public static class ConfidenceInterval {

    public final double lowerBound;
    public final double upperBound;

    public ConfidenceInterval(double lowerBound, double upperBound) {
      this.lowerBound = lowerBound;
      this.upperBound = upperBound;
    }

    @Override
    public String toString() {
      return String.format("[%+.3f, %+.3f]", lowerBound, upperBound);
    }
  }
}
