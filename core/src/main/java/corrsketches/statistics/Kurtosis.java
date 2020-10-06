package corrsketches.statistics;

/** Implements multiple Kurtosis estimators. */
public class Kurtosis {

  /**
   * Computes the sample excess kurtosis of the given vector.
   *
   * @param x an input vector.
   * @return the sample excess Kurtosis.
   */
  public static double g2(double[] x) {
    final int n = x.length;
    double mean = 0.0, m2 = 0.0, m4 = 0.0;
    for (int i = 0; i < n; i++) {
      mean += x[i];
    }
    mean /= n;

    for (int i = 0; i < n; i++) {
      double xt = x[i] - mean;
      m2 += xt * xt;
      m4 += xt * xt * xt * xt;
    }
    m2 /= n;
    m4 /= n;
    final double g2 = (m4 / (m2 * m2)) - 3.;
    return g2;
  }

  /** Computes the excess kurtosis using the G2 estimator. */
  public static double G2(final double[] x) {
    final int n = x.length;
    if (n < 4) {
      return Double.NaN;
    }
    final double G2 = ((n - 1) / (double) ((n - 2) * (n - 3))) * (((n + 1) * g2(x)) + 6.);
    return G2;
  }

  /**
   * Implements the kc kurtosis estimator proposed in the paper "Estimating kurtosis and confidence
   * intervals for the variance under non-normality", Journal of Statistical Computation and
   * Simulation, 2013.
   */
  public static double kc(final double[] x, int c) {
    final int n = x.length;
    final double G2 = G2(x);
    double k5 = ((n + 1) / (double) (n - 1)) * G2 * (1 + c * G2 / (double) n);
    return k5;
  }

  /**
   * Implements the k5 kurtosis estimator proposed in the paper "Estimating kurtosis and confidence
   * * intervals for the variance under non-normality", Journal of Statistical Computation and *
   * Simulation, 2013.
   */
  public static double k5(final double[] x) {
    return kc(x, 5);
  }
}
