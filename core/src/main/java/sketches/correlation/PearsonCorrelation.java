package sketches.correlation;

import smile.stat.distribution.GaussianDistribution;
import smile.stat.distribution.TDistribution;

public class PearsonCorrelation {

  /**
   * Computes the Pearson product-moment correlation coefficient for two vectors. When the vector
   * covariances are zero (i.e., the series are constant) this implementation returns Double.NaN.
   *
   * <p>Implementation adapted from ELKI toolkit:
   * https://github.com/elki-project/elki/blob/master/elki-core-math/src/main/java/de/lmu/ifi/dbs/elki/math/PearsonCorrelation.java
   *
   * @param x first vector
   * @param y second vector
   * @return the Pearson product-moment correlation coefficient for x and y.
   */
  public static double coefficient(double[] x, double[] y) {
    final int xdim = x.length;
    final int ydim = y.length;
    if (xdim != ydim) {
      throw new IllegalArgumentException("Invalid arguments: arrays differ in length.");
    }
    if (xdim == 0) {
      throw new IllegalArgumentException("Empty vector.");
    }
    // Inlined computation of Pearson correlation, to avoid allocating objects!
    // This is a numerically stabilized version, avoiding sum-of-squares.
    double sumXX = 0., sumYY = 0., sumXY = 0.;
    double sumX = x[0], sumY = y[0];
    int i = 1;
    while (i < xdim) {
      final double xv = x[i], yv = y[i];
      // Delta to previous mean
      final double deltaX = xv * i - sumX;
      final double deltaY = yv * i - sumY;
      // Increment count first
      final double oldi = i; // Convert to double!
      ++i;
      final double f = 1. / (i * oldi);
      // Update
      sumXX += f * deltaX * deltaX;
      sumYY += f * deltaY * deltaY;
      // should equal deltaY * deltaX!
      sumXY += f * deltaX * deltaY;
      // Update sums
      sumX += xv;
      sumY += yv;
    }
    // One or both series were constant:
    if (!(sumXX > 0. && sumYY > 0.)) {
      return Double.NaN;
    }
    return sumXY / Math.sqrt(sumXX * sumYY);
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
   * Given a Pearson correlation coefficient and the sample size, this fucntion computes the p-value
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
    //        double criticalR = criticalR(degreesOfFreedom, criticalT);
    //        double probability = tDistribution.cdf(tValue);
    //        System.out.printf(
    //            "significance=%.2f  pearson=%.2f df=%d t=%.3f  criticalT=%.3f  criticalR=%.3f
    // prob=%.2f  sample-size=%d  result=%s\n",
    //            significance,
    //            coefficient,
    //            degreesOfFreedom,
    //            tValue,
    //            criticalT,
    //            criticalR,
    //            probability,
    //            sampleSize,
    //            String.valueOf(criticalT < tValue)
    //        );
    return tScore >= criticalT;
  }

  private static double tScore(double coefficient, int sampleSize) {
    double r = Math.abs(coefficient); // TODO: is this correct?
    int n = sampleSize;
    double t = r * Math.sqrt((n - 2) / (1 - r * r));
    return t;
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
    double alpha = (1. - confidence) / 2;
    double z = new GaussianDistribution(0, 1).quantile(1. - alpha);
    double zstderr = 1 / Math.sqrt(n - 3);
    double interval = z * zstderr;
    double zp = rtoz(r);
    double ubz = zp + interval;
    double lbz = zp - interval;
    double ub = ztor(ubz);
    double lb = ztor(lbz);
    //        System.out.printf(
    //                "r=%.2f  n=%d confidence=%.2f zp=%.3f  zstderr=%.3f  z=%.3f  interval=%.3f
    // ubz=%.3f  lbz=%.3f  ub=%.3f  lb=%.3f\n",
    //                r, n, confidence, zp, zstderr, z, interval, ubz, lbz, ub, lb
    //        );
    return new ConfidenceInterval(lb, ub);
  }

  /**
   * Computes Fisher's transformation function from r to z.
   *
   * @param r the sample Pearson's correlation coefficient
   * @return z, the Fisher's z-transformation
   */
  private static double rtoz(double r) {
    double z = 0.5 * Math.log((1 + r) / (1 - r));
    return z;
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

    double lowerBound;
    double upperBound;

    public ConfidenceInterval(double lowerBound, double upperBound) {
      this.lowerBound = lowerBound;
      this.upperBound = upperBound;
    }

    @Override
    public String toString() {
      return String.format("[%+.3f, %+.3f]", lowerBound, upperBound);
    }
  }

  //    public boolean isSignificant(int degreesOfFreedom, double tScore, double
  // levelOfSignificance) {
  //        double cProb = new TDistribution(degreesOfFreedom).cdf(tScore);
  //        return cProb > levelOfSignificance;
  //    }

}
