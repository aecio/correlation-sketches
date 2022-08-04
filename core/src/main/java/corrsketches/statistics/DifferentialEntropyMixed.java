package corrsketches.statistics;

import static corrsketches.statistics.Stats.sum;
import static java.lang.Math.abs;
import static java.lang.Math.log;
import static smile.math.special.Gamma.digamma;

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
    NeighborhoodCount nn = new NeighborhoodCount();
    double[] distances = new double[x.length];
    for (int i = 0; i < x.length; i++) {
      kthNearestNonZero(x, i, k, nn);
      //      distances[i] = digamma(N) - digamma(nn.k) + LOG_CD + log(nn.distance);
      distances[i] = log(nn.distance) - digamma(nn.k);
    }
    return digamma(N) + LOG_CD + (1 / N) * sum(distances);
  }

  static NeighborhoodCount kthNearestNonZero(double[] data, final int target, int k) {
    NeighborhoodCount nn = new NeighborhoodCount();
    kthNearestNonZero(data, target, k, nn);
    return nn;
  }

  static void kthNearestNonZero(double[] data, final int target, int k, NeighborhoodCount nn) {
    //    System.out.println("DifferentialEntropyMixed.kthNearest");

    // k must be at most the size of the input minus 1
    final int maxK = data.length - 1;
    int localK = Math.min(k, maxK);

    double c = data[target];
    int left = target;
    int right = target;
    int theNeighbor = -1; // initial value not used, but needed to make compiler happy

    for (int i = 0; i < localK; i++) {
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
      //      System.out.println("i = " + i + " theN = " + theNeighbor + " data=" +
      // data[theNeighbor]);
    }
    double distance = abs(data[theNeighbor] - c);
    if (distance == 0) {
      //      System.out.println("Computing distance for discrete...");
      while (localK <= maxK && distance == 0) {
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
        localK++;
        distance = abs(data[theNeighbor] - c);
      }
    }
    nn.k = localK;
    nn.distance = distance;
    nn.kthNearest = data[theNeighbor];
    //    if (k != localK) {
    //      System.out.println("initial_k=" + k + " local_k=" + localK+ " dist=" + distance);
    //    }
  }

  static class NeighborhoodCount {

    int k;
    double kthNearest;
    double distance;
  }
}
