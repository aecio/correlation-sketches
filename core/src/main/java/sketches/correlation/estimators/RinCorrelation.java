package sketches.correlation.estimators;

import sketches.correlation.PearsonCorrelation;
import sketches.statistics.Stats;
import smile.sort.QuickSort;

/**
 * Implements the Rank-Based Inverse Normal (RIN) Transformation correlation coefficient
 */
public class RinCorrelation {

  /**
   * Applies the RIN transformation to the input vectors and computes the Pearson's correlation
   * coefficient of transformed values. The RIN transformation produces approximate normality in the
   * sample regardless of the original distribution shape, so long as ties are rare and the sample
   * size is reasonable.
   */
  public static double coefficient(double[] x, double[] y) {

    if (x.length != y.length) {
      throw new IllegalArgumentException("Input vector sizes are different.");
    }

    int n = x.length;
    double[] a = new double[n];
    double[] b = new double[n];
    for (int j = 0; j < n; j++) {
      a[j] = x[j];
      b[j] = y[j];
    }

    QuickSort.sort(a, b);
    rank(a);
    rankit(a);

    QuickSort.sort(b, a);
    rank(b);
    rankit(b);

    final double rrin = PearsonCorrelation.coefficient(a, b);
    return rrin;
  }

  /**
   * The rankit function applied to rank values can produce approximate normality in the sample
   * regardless of the original distribution shape, so long as ties are rare and the sample size is
   * reasonable.
   */
  private static void rankit(double[] x) {
    final int n = x.length;
    for (int i = 0; i < n; i++) {
      x[i] = Stats.NORMAL.quantile((x[i] - .5) / n);
    }
  }

  /**
   * Given a sorted array, replaces the elements by their rank. When values are tied, they are
   * assigned the mean of their ranks.
   *
   * @param x - an sorted array
   */
  private static void rank(double[] x) {
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
        // replaces tied values by the mean of their rank
        double rank = 0.5 * (j + jt - 1);
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

}
