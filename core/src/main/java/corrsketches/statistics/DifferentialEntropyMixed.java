package corrsketches.statistics;

import static corrsketches.statistics.NearestNeighbors1D.kthNearestNonZero;
import static corrsketches.statistics.Stats.sum;
import static java.lang.Math.log;
import static smile.math.special.Gamma.digamma;

import corrsketches.statistics.NearestNeighbors1D.NearestNeighbor;
import java.util.Arrays;

/**
 * This class implement a variation of the Kozachenko-Leonenko estimator for the differential
 * entropy (for numerical values) that handles repeated values (i.e., the data is mixture of
 * discrete-continuous distributions) using the trick described in the paper:
 *
 * <p><i>Gao, W., Kannan, S., Oh, S. and Viswanath, P., 2017. Estimating mutual information for
 * discrete-continuous mixtures. Advances in neural information processing systems, 30.<i/>
 */
public class DifferentialEntropyMixed {

  public static final double LOG_CD = log(2);

  public static double entropy(double[] x) {
    return entropy(x, 3);
  }

  /**
   * Estimates the differential entropy of a random variable {@param x} (in nats) based on the
   * kth-nearest neighbour distances between point samples. This function implements a variation of
   * the Kozachenko-Leonenko (1987) that handles repeated values (i.e., the data is mixture of
   * discrete-continuous distributions) using the trick described in the paper:
   *
   * <p><i>Gao, W., Kannan, S., Oh, S. and Viswanath, P., 2017. Estimating mutual information for
   * discrete-continuous mixtures. Advances in neural information processing systems, 30.<i/>
   *
   * @param x the random variable
   * @param k the number of k nearest neighbors
   * @return the entropy estimate
   */
  public static double entropy(double[] x, final int k) {
    final double N = x.length;
    x = Arrays.copyOf(x, x.length);
    Arrays.sort(x);
    NearestNeighbor nn = new NearestNeighbor();
    double[] distances = new double[x.length];
    for (int i = 0; i < x.length; i++) {
      kthNearestNonZero(x, i, k, nn);
      if (nn.distance == 0.0) {
        // if the returned distance is zero, then all points are equal
        // and the result will be -Infinity since log(0) = -Infinity.
        return Double.NEGATIVE_INFINITY;
      }
      distances[i] = log(nn.distance) - digamma(nn.k);
    }
    return digamma(N) + LOG_CD + (1 / N) * sum(distances);
  }
}
