package corrsketches.correlation;

import static corrsketches.statistics.Stats.rank;

import smile.sort.QuickSort;

/** Implements Spearman's correlation coefficient. */
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
}
