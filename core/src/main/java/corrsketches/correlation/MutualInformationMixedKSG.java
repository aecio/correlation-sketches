package corrsketches.correlation;

import static java.lang.Math.log;
import static smile.math.special.Gamma.digamma;

import corrsketches.statistics.Stats;
import corrsketches.util.KDTree;
import corrsketches.util.KDTree.Distance;
import corrsketches.util.KDTree.Neighbor;
import java.util.Random;

/**
 * Implements the Mutual Information estimator for mixtures of continuous and discrete numerical
 * variables (MixedKSG), as described in the paper:
 *
 * <ul>
 *   <i>Gao, W., Kannan, S., Oh, S. and Viswanath, P., 2017. Estimating mutual information for
 *   discrete-continuous mixtures. Advances in neural information processing systems, 30.<i/>
 * </ul>
 *
 * <p>The MixedKSG estimator is modification of the traditional KSG estimator that deals with the
 * degenerate case in which the numerical variables contain repeated discrete values.
 */
public class MutualInformationMixedKSG {

  public static final int DEFAULT_K = 3;
  public static final int DEFAULT_SEED = 9;

  public static double mi(double[] x, double[] y) {
    return mi(x, y, DEFAULT_K);
  }

  public static double miWithNoise(double[] x, double[] y) {
    return Math.max(0, miRaw(x, y, DEFAULT_K, new Random(DEFAULT_SEED)));
  }

  /**
   * Computes the mutual information between the array {@param x} and the array {@param y}.
   *
   * @param x a vector of x variables
   * @param y a vector of y variables
   * @param k the number k-nearest neighbors
   * @return the mutual information estimate.
   */
  public static double mi(final double[] x, final double[] y, int k) {
    return Math.max(0, miRaw(x, y, k));
  }

  static double miRaw(final double[] x, final double[] y, int k) {
    return miRaw(x, y, k, null);
  }
  /**
   * Computes the mutual information between the array {@param x} and the array {@param y}.
   *
   * @param x a vector of x variables
   * @param y a vector of y variables
   * @param k the number k-nearest neighbors
   * @param rng (Optional) if not null, this random number generator is used to add small Gaussian
   *     noise to the variables. This should make this estimator behave like the original KSG
   *     estimator, given that discrete points (ties) are resolved using random noise.
   * @return the mutual information estimate.
   */
  static double miRaw(final double[] x, final double[] y, int k, Random rng) {
    if (x.length != y.length) {
      throw new IllegalArgumentException("Arrays x and y must have the same length.");
    }
    final int N = x.length;
    final double[][] xy = new double[N][2];
    final double[][] x1 = new double[N][1];
    final double[][] y1 = new double[N][1];

    k = Math.min(k, x.length - 1); // k cannot exceed the vector size -1

    if (rng != null) {
      // If the random number generator is provided, we add random gaussian noise. According to the
      // paper, when there are no ties, the MixedKSG method recovers the original KSG method.
      double xm = Math.max(1, Stats.mean(x));
      double ym = Math.max(1, Stats.mean(y));
      for (int i = 0; i < N; i++) {
        final double xi = x[i] + rng.nextGaussian() * xm * 1e-10;
        final double yi = y[i] + rng.nextGaussian() * ym * 1e-10;
        xy[i][0] = xi;
        xy[i][1] = yi;
        x1[i][0] = xi;
        y1[i][0] = yi;
      }
    } else {
      // Otherwise, just convert the 1-d vectors to 2-d vectors for use with the KDTree.
      for (int i = 0; i < N; i++) {
        xy[i][0] = x[i];
        xy[i][1] = y[i];
        x1[i][0] = x[i];
        y1[i][0] = y[i];
      }
    }

    KDTree xyTree = new KDTree(xy, Distance.CHEBYSHEV);
    KDTree xTree = new KDTree(x1, Distance.CHEBYSHEV);
    KDTree yTree = new KDTree(y1, Distance.CHEBYSHEV);

    double[] distances = new double[N];
    for (int i = 0; i < N; i++) {
      Neighbor[] neighbors = xyTree.knn(xy[i], k + 1); // k+1 because returns the query itself
      distances[i] = neighbors[0].distance; // index 0 is the largest distance
    }

    double mi = 0.0;
    for (int i = 0; i < N; i++) {
      final double kthDistance = distances[i]; // the k-th smallest distance to point i
      final int kp;
      final double radius;
      if (kthDistance == 0) {
        radius = Math.nextUp(0); // smallest non-zero
        kp = xyTree.countInRange(xy[i], radius);
      } else {
        radius = Math.nextDown(kthDistance);
        kp = k;
      }
      // the kd-tree counts 1 for the query itself, so the values of nx and ny
      // actually are equal nx + 1 and ny + 1 from the paper
      final int nx = xTree.countInRange(x1[i], radius);
      final int ny = yTree.countInRange(y1[i], radius);

      mi += (digamma(kp) - digamma(nx) - digamma(ny)) / N;
    }
    mi += log(N);
    return mi;
  }
}
