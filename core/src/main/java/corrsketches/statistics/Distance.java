package corrsketches.statistics;

public class Distance {

  /**
   * Chebyshev distance (aka, maximum metric, or L∞ metric) is a metric defined on a vector space
   * where the distance between two vectors is the greatest of their differences along any
   * coordinate dimension.
   *
   * @return the chebyshev distance between the vectors {@param x} and {@param y}
   */
  public static double chebyshev(double[] x, double[] y) {
    int n = x.length;
    double[] diff = new double[n];
    for (int i = 0; i < n; i++) {
      diff[i] = x[i] - y[i];
    }
    double d = Math.abs(diff[0]);
    for (int i = 1; i < n; i++) {
      d = Math.max(d, Math.abs(diff[i]));
    }
    return d;
  }

  /**
   * The Euclidean distance between two points in Euclidean space is the length of a line segment
   * between the two points. It is computed as: d(x,y) = \sqrt{|x-y|}.
   *
   * @return the euclidean distance between the vectors {@param x} and {@param y}
   */
  public static double euclidean(double[] x, double[] y) {
    double sum = 0.0;
    for (int i = 0; i < x.length; i++) {
      final double diff = x[i] - y[i];
      sum += diff * diff;
    }
    return Math.sqrt(sum);
  }
}
