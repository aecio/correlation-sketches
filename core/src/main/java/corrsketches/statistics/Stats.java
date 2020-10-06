package corrsketches.statistics;

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
    return mean(x, x.length);
  }

  public static double mean(double[] x, int n) {
    double sum = 0.0;
    for (int i = 0; i < n; i++) {
      sum += x[i];
    }
    return sum / n;
  }

  /**
   * Computes minimum and maximum values of an array.
   *
   * @return the extent (min and max) of the array
   */
  public static Extent extent(final double[] xarr) {
    double max = Double.NEGATIVE_INFINITY;
    double min = Double.POSITIVE_INFINITY;
    for (int i = 0; i < xarr.length; i++) {
      final double x = xarr[i];
      if (x < min) {
        min = x;
      }
      if (x > max) {
        max = x;
      }
    }
    return new Extent(min, max);
  }

  public static double[] unitRange(double[] x, double min, double max) {
    double[] xu = new double[x.length];
    for (int i = 0; i < x.length; i++) {
      xu[i] = (x[i] - min) / (max - min);
    }
    return xu;
  }

  public static class Extent {

    public final double min;
    public final double max;

    public Extent(double min, double max) {
      this.min = min;
      this.max = max;
    }
  }

  /**
   * Computes a dot product between vectors x and y and divides the result by the length of the
   * vectors:
   *
   * <pre>
   * 1/n * &lt;x, y&gt;,
   * </pre>
   *
   * where n is the length of vector x, and &lt;x, y&gt; denotes the dot product between x and y.
   * This function assumes that both x and y have the same length.
   */
  public static double dotn(double[] x, double[] y) {
    assert x.length == y.length;
    final int n = x.length;
    double sum = 0;
    for (int i = 0; i < n; i++) {
      sum += x[i] * y[i];
    }
    return sum / n;
  }
}
