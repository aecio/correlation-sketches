package corrsketches.statistics;

import static corrsketches.statistics.Stats.sum;
import static java.lang.Math.abs;
import static smile.math.special.Gamma.digamma;

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
    for (int i = 0; i < x.length; i++) {
      double kth = kthNearest(x, i, k);
      distances[i] = abs(x[i] - kth);
    }
    return distances;
  }

  static double kthNearest(double[] data, int target, int k) {
    // k must be at most the size of the input minus 1
    k = Math.min(k, data.length - 1);

    double c = data[target];
    int left = target;
    int right = target;
    int theNeighbor = -1; // initial value not used, but needed to make compiler happy

    for (int i = 0; i < k; i++) {
      if (left == 0) {
        right++;
        theNeighbor = right;
      } else if (right == data.length - 1) {
        left--;
        theNeighbor = left;
      } else if (abs(data[left - 1] - c) < abs(data[right + 1] - c)) {
        left--;
        theNeighbor = left;
      } else {
        right++;
        theNeighbor = right;
      }
    }
    return data[theNeighbor];
  }

  static double distanceToKthNearest(double[] data, int target, int k) {
    final double c = data[target];
    final double kthNearest = kthNearest(data, target, k);
    return Math.abs(kthNearest - c);
  }
}
