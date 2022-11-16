package corrsketches.statistics;

import static java.lang.Math.log;
import static java.lang.Math.max;

import java.util.Arrays;
import java.util.Random;
import smile.stat.distribution.GaussianDistribution;

public class Stats {

  public static final GaussianDistribution NORMAL = new GaussianDistribution(0, 1);

  /**
   * Computes the covariance between the two input vectors.
   *
   * @return cov(x, y)
   */
  public static double cov(double[] x, double[] y) {
    final double meanX = mean(x);
    final double meanY = mean(y);
    double sumXY = 0.0;
    for (int i = 0; i < x.length; i++) {
      sumXY += (x[i] - meanX) * (y[i] - meanY);
    }
    return sumXY / (x.length - 1);
  }

  /**
   * Computes the mean of the given input vector.
   *
   * @return sum(x)/n
   */
  public static double mean(long[] x) {
    long sum = 0;
    for (long l : x) {
      sum += l;
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

  public static double median(double[] x) {
    return median(x, x.length, false);
  }

  public static double median(double[] x, int n) {
    return median(x, n, false);
  }

  public static double median(double[] x, int n, boolean inplace) {
    if (n == 1) {
      return x[0];
    }
    if (n == 2) {
      return (x[0] + x[1]) / 2;
    }

    if (!inplace) {
      x = Arrays.copyOf(x, n);
    }
    Arrays.sort(x, 0, n);

    double median;
    if (n % 2 == 0) {
      median = (x[n / 2 - 1] + x[n / 2]) / 2.0;
    } else {
      median = x[n / 2];
    }

    return median;
  }

  /**
   * Computes minimum and maximum values of an array.
   *
   * @return the extent (min and max) of the array
   */
  public static Extent extent(final double[] xarr) {
    double max = Double.NEGATIVE_INFINITY;
    double min = Double.POSITIVE_INFINITY;
    for (final double x : xarr) {
      if (x < min) {
        min = x;
      }
      if (x > max) {
        max = x;
      }
    }
    return new Extent(min, max);
  }

  public static double[] unitize(double[] x, double min, double max) {
    double[] xu = new double[x.length];
    for (int i = 0; i < x.length; i++) {
      xu[i] = (x[i] - min) / (max - min);
    }
    return xu;
  }

  public static double[] unitize(double[] x) {
    Extent ext = extent(x);
    return unitize(x, ext.min, ext.max);
  }

  /** Computes the (uncorrected) standard deviation, also known as the mean squared deviations. */
  public static double std(double[] x) {
    final double n = x.length;
    if (n == 0) {
      return 0.0;
    }
    double sum = 0.0;
    for (double xi : x) {
      sum += xi;
    }
    final double mean = sum / n;
    double dev;
    sum = 0.0;
    for (double xi : x) {
      dev = xi - mean;
      sum += dev * dev;
    }
    return Math.sqrt(sum / n);
  }

  /**
   * Standardize the vector {@param x} by removing the mean and scaling to unit variance. The
   * standard score of a sample x is calculated as:
   *
   * <p>z = (x - u) / s
   *
   * <p>where u is the mean of {@param x} and s is the standard deviation of {@param x}.
   *
   * @param x the input vector
   * @return the standardized vector z
   */
  public static double[] standardize(double[] x) {
    final double stdx = std(x);
    final double meanx = mean(x);
    final int n = x.length;
    double[] result = new double[n];
    for (int i = 0; i < n; i++) {
      result[i] = (x[i] - meanx) / stdx;
    }
    return result;
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

  /**
   * Given a sorted array, replaces the elements by their rank. When values are tied, they are
   * assigned the mean of ranks of the tied values.
   *
   * @param x a sorted array
   */
  public static void rank(double[] x) {
    rank(x, TiesMethod.AVERAGE);
  }

  /**
   * Given a sorted array, replaces the elements by their rank. When values are tied, they are
   * assigned a ranked computed using one of the methods in {@code TiesMethod} enum.
   *
   * @param x a sorted array
   * @param tiesMethod the method to be used to compute the rank of tied values
   */
  public static void rank(double[] x, TiesMethod tiesMethod) {
    int n = x.length;
    int j = 1;
    while (j < n) {
      if (x[j] != x[j - 1]) {
        x[j - 1] = j;
        ++j;
      } else {
        // find all ties
        int jt = j + 1;
        while (jt <= n && x[jt - 1] == x[j - 1]) {
          jt++;
        }
        // compute the rank to replace ties according to the chosen method for handling ties
        double rank;
        switch (tiesMethod) {
          case AVERAGE:
            rank = 0.5 * (j + jt - 1);
            break;
          case MAX:
            rank = jt - 1;
            break;
          case MIN:
            rank = j;
            break;
          default:
            throw new IllegalArgumentException("Unsupported method: " + tiesMethod);
        }
        // replaces tied values according to the computed rank
        for (int ji = j; ji <= (jt - 1); ji++) {
          x[ji - 1] = rank;
        }
        // advance to next untied result
        j = jt;
      }
    }

    if (j == n) {
      x[n - 1] = n;
    }
  }

  public static double sum(final double[] x) {
    return sum(x, x.length);
  }

  public static double sum(final double[] x, int xLength) {
    double sum = 0d;
    for (int i = 0; i < xLength; i++) {
      sum += x[i];
    }
    return sum;
  }

  /**
   * Bins the {@code data} using a heuristic to choose the number of categories B. The value of B is
   * chosen to be equal to max(2, log(U)), where U denotes the number of unique values in the {@code
   * data} vector.
   *
   * <p>This heuristic has been previously used in the paper: Dougherty, J., Kohavi, R. and Sahami,
   * M., 1995. Supervised and unsupervised discretization of continuous features. In Machine
   * learning proceedings 1995 (pp. 194-202). Morgan Kaufmann.
   *
   * @param data the vector to be binned
   * @return the binned version of the vector {@code data}
   */
  public static int[] binEqualWidth(double[] data) {
    double[] sorted = Arrays.copyOf(data, data.length);
    Arrays.sort(data);
    int unique = 1;
    double min = sorted[0];
    double max = sorted[0];
    for (int i = 1; i < sorted.length; i++) {
      if (sorted[i - 1] != sorted[i]) {
        unique++;
      }
      if (sorted[i] < min) {
        min = sorted[i];
      }
      if (sorted[i] > max) {
        max = sorted[i];
      }
    }
    return binEqualWidth(sorted, (int) max(2, log(unique)), min, max);
  }

  public static int[] binEqualWidth(double[] data, int bins) {
    Extent extent = extent(data);
    return binEqualWidth(data, bins, extent.min, extent.max);
  }

  public static int[] binEqualWidth(double[] data, int bins, double min, double max) {
    final int[] binned = new int[data.length];
    for (int i = 0; i < data.length; i++) {
      binned[i] = (int) (((data[i] - min) / (max - min) * (bins - 1)) + 0.5);
    }
    return binned;
  }

  public static double[] addRandomNoise(double[] x) {
    return addRandomNoise(x, new Random());
  }

  public static double[] addRandomNoise(double[] x, long seed) {
    return addRandomNoise(x, new Random(seed));
  }

  public static double[] addRandomNoise(double[] x, Random rng) {
    final double TINY = 1e-20;
    final double mean = mean(x);
    //    final double mean = 1;
    double[] xn = Arrays.copyOf(x, x.length);
    for (int i = 0; i < xn.length; i++) {
      xn[i] = xn[i] + TINY * mean * rng.nextGaussian();
    }
    return xn;
  }

  public static double[] toProbabilities(double[] data) {
    final double sum = Math.nextUp(sum(data));
    double[] probabilities = new double[data.length];
    for (int i = 0; i < data.length; i++) {
      probabilities[i] = data[i] / sum;
    }
    return probabilities;
  }

  public enum TiesMethod {
    AVERAGE,
    MAX,
    MIN
  }

  public static class Extent {

    public final double min;
    public final double max;

    public Extent(double min, double max) {
      this.min = min;
      this.max = max;
    }

    @Override
    public String toString() {
      return "Extent(min=" + min + ", max=" + max + ')';
    }
  }
}
