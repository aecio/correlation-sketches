package corrsketches.correlation;

import corrsketches.correlation.Correlation.Estimate;
import smile.sort.QuickSort;

public class SpearmanCorrelation {

  public static double coefficient(double[] x, double[] y) {
    return spearman(x, y);
  }

  public static Estimate estimate(double[] x, double[] y) {
    return new Estimate(spearman(x, y), x.length);
  }

  public static double spearman(double[] x, double[] y) {
    if (x.length != y.length) {
      throw new IllegalArgumentException("Input vector sizes are different.");
    }

    double[] a = x.clone();
    double[] b = y.clone();

    QuickSort.sort(a, b);
    rank(a);
    QuickSort.sort(b, a);
    rank(b);

    return PearsonCorrelation.coefficient(a, b);
  }

  /**
   * Given a sorted array, replaces the elements by their rank. When values are tied, they are
   * assigned the mean of their ranks.
   *
   * @param x - an sorted array
   */
  protected static void rank(double[] x) {
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
