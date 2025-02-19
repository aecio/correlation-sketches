package corrsketches.correlation;

import java.util.Arrays;
import java.util.Comparator;

/**
 * Implements the Kendall's Tau Correlation using an algorithm that runs in O(n*log n) time. This
 * implementation is based on code from Apache's commons-math project available at:
 * https://github.com/apache/commons-math/blob/924f6c357465b39beb50e3c916d5eb6662194175/commons-math-legacy/src/main/java/org/apache/commons/math4/legacy/stat/correlation/KendallsCorrelation.java
 * The original commons-math's code is licensed under Apache License, version 2.0.
 */
public class KendallTauCorrelation {

  @SuppressWarnings("ComparatorCombinators")
  static final Comparator<PairDD> PAIR_COMPARATOR =
      (pair1, pair2) -> {
        int compareFirst = Double.compare(pair1.x, pair2.x);
        return compareFirst != 0 ? compareFirst : Double.compare(pair1.y, pair2.y);
      };

  public static double correlation(final double[] x, final double[] y) {

    if (x.length != y.length) {
      throw new IllegalArgumentException(
          String.format("Dimensions do not match: x=%d y=%d", x.length, y.length));
    }

    final int n = x.length;
    final long numPairs = sum(n - 1);

    PairDD[] pairs = new PairDD[n];
    for (int i = 0; i < n; i++) {
      pairs[i] = new PairDD(x[i], y[i]);
    }

    Arrays.sort(pairs, PAIR_COMPARATOR);

    long tiedXPairs = 0;
    long tiedXYPairs = 0;
    long consecutiveXTies = 1;
    long consecutiveXYTies = 1;
    PairDD prev = pairs[0];
    for (int i = 1; i < n; i++) {
      final PairDD curr = pairs[i];
      if (Double.compare(curr.x, prev.x) == 0) {
        consecutiveXTies++;
        if (Double.compare(curr.y, prev.y) == 0) {
          consecutiveXYTies++;
        } else {
          tiedXYPairs += sum(consecutiveXYTies - 1);
          consecutiveXYTies = 1;
        }
      } else {
        tiedXPairs += sum(consecutiveXTies - 1);
        consecutiveXTies = 1;
        tiedXYPairs += sum(consecutiveXYTies - 1);
        consecutiveXYTies = 1;
      }
      prev = curr;
    }
    tiedXPairs += sum(consecutiveXTies - 1);
    tiedXYPairs += sum(consecutiveXYTies - 1);

    long swaps = 0;

    PairDD[] pairsDestination = new PairDD[n];
    for (int segmentSize = 1; segmentSize < n; segmentSize <<= 1) {
      for (int offset = 0; offset < n; offset += 2 * segmentSize) {
        int i = offset;
        final int iEnd = Math.min(i + segmentSize, n);
        int j = iEnd;
        final int jEnd = Math.min(j + segmentSize, n);

        int copyLocation = offset;
        while (i < iEnd || j < jEnd) {
          if (i < iEnd) {
            if (j < jEnd) {
              if (Double.compare(pairs[i].y, pairs[j].y) <= 0) {
                pairsDestination[copyLocation] = pairs[i];
                i++;
              } else {
                pairsDestination[copyLocation] = pairs[j];
                j++;
                swaps += iEnd - i;
              }
            } else {
              pairsDestination[copyLocation] = pairs[i];
              i++;
            }
          } else {
            pairsDestination[copyLocation] = pairs[j];
            j++;
          }
          copyLocation++;
        }
      }
      final PairDD[] pairsTemp = pairs;
      pairs = pairsDestination;
      pairsDestination = pairsTemp;
    }

    long tiedYPairs = 0;
    long consecutiveYTies = 1;
    prev = pairs[0];
    for (int i = 1; i < n; i++) {
      final PairDD curr = pairs[i];
      if (Double.compare(curr.y, prev.y) == 0) {
        consecutiveYTies++;
      } else {
        tiedYPairs += sum(consecutiveYTies - 1);
        consecutiveYTies = 1;
      }
      prev = curr;
    }
    tiedYPairs += sum(consecutiveYTies - 1);

    final long concordantMinusDiscordant =
        numPairs - tiedXPairs - tiedYPairs + tiedXYPairs - 2 * swaps;
    final double nonTiedPairsMultiplied =
        (numPairs - tiedXPairs) * (double) (numPairs - tiedYPairs);
    return concordantMinusDiscordant / Math.sqrt(nonTiedPairsMultiplied);
  }

  /**
   * Returns the sum of the number from 1...n according to Gauss' summation formula: \[
   * \sum\limits_{k=1}^n k = \frac{n(n + 1)}{2} \]
   *
   * @param n the summation end
   * @return the sum of the number from 1 to n
   */
  private static long sum(long n) {
    return n * (n + 1) / 2L;
  }

  /**
   * Implements the transformation of Kendall's tau coefficient to the Mutual Information between
   * numerical variables transformed using the Kendall's transformation, as described in the paper:
   * "Kendall transformation: a robust representation of continuous data for information theory" by
   * Miron Bartosz Kursa (https://arxiv.org/abs/2006.15991).
   *
   * @param tau
   * @return the mutual information of the Kendall-transformed numerical variables used to compute
   *     the given tau correlation.
   */
  public static double kendallToMI(double tau) {
    return tau * Math.log(Math.sqrt((1 + tau) / (1 - tau))) + Math.log(Math.sqrt(1 - tau * tau));
  }

  private static class PairDD {
    double x;
    double y;

    PairDD(double x, double y) {
      this.x = x;
      this.y = y;
    }
  }
}
