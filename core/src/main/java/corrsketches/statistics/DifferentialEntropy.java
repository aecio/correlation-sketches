package corrsketches.statistics;

import static corrsketches.statistics.Stats.sum;
import static smile.math.special.Gamma.digamma;

import corrsketches.statistics.NearestNeighbors1D.NearestNeighbor;
import java.util.Arrays;

/**
 * This class implements the classic Kozachenko-Leonenko (1987) estimator for the differential
 * entropy (for numerical values). The implementation follows the description in the paper:
 *
 * <p><i>D. Lombardi, S. Pant. Nonparametric k-nearest-neighbor entropy estimator. Physical Review
 * E. 93 (2016).</i>
 */
public class DifferentialEntropy {

  public static double entropy(double[] x) {
    return entropy(x, 3);
  }

  /**
   * Estimates the differential entropy of a random variable {@param x} (in nats) based on the
   * kth-nearest neighbour distances between point samples. This function implements the
   * Kozachenko-Leonenko (1987) estimator as described in the paper: "Nonparametric
   * k-nearest-neighbor entropy estimator" by D. Lombardi, S. Pant. Physical Review E. 93 (2016).
   *
   * @param x the random variable
   * @param k the number of k nearest neighbors
   * @return the entropy estimate
   */
  public static double entropy(double[] x, final int k) {
    if (x.length <= 1) {
      return Double.NEGATIVE_INFINITY;
    }
    final double N = x.length;
    double[] distances = computeDistances(x, k);
    for (int i = 0; i < distances.length; i++) {
      distances[i] = Math.log(distances[i]);
    }
    // This implementation x has fixed dimension d=1 and uses maximum norm.
    // Thus, log(c^d) = log(2).
    final double log_cd = Math.log(2); // log of volume of the d-dimensional unit-ball, where d=1
    return digamma(N) - digamma(k) + log_cd + (1 / N) * sum(distances);
  }

  private static double[] computeDistances(double[] x, int k) {
    x = Arrays.copyOf(x, x.length);
    Arrays.sort(x);
    double[] distances = new double[x.length];
    NearestNeighbor nn = new NearestNeighbor();
    for (int i = 0; i < x.length; i++) {
      NearestNeighbors1D.kthNearest(x, i, k, nn);
      distances[i] = nn.distance;
    }
    return distances;
  }
}
