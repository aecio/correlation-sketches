package corrsketches.correlation;

import static corrsketches.statistics.Stats.*;

import smile.sort.QuickSort;

/** Implements the Rank-Based Inverse Normal (RIN) Transformation correlation coefficient. */
public class RinCorrelation {

  /**
   * Applies the RIN transformation to the input vectors and computes the Pearson's correlation
   * coefficient of transformed values. The RIN transformation produces approximate normality in the
   * sample regardless of the original distribution shape, so long as ties are rare and the sample
   * size is reasonable.
   */
  public static Estimate estimate(double[] x, double[] y) {
    final double rin = coefficient(x, y);
    return new Estimate(rin, x.length);
  }

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

    double[] a = x.clone();
    double[] b = y.clone();

    QuickSort.sort(a, b);
    rank(a);
    rankit(a);

    QuickSort.sort(b, a);
    rank(b);
    rankit(b);

    return PearsonCorrelation.coefficient(a, b);
  }

  /**
   * The rankit function applied to rank values can produce approximate normality in the sample
   * regardless of the original distribution shape, so long as ties are rare and the sample size is
   * reasonable.
   */
  private static void rankit(double[] x) {
    final int n = x.length;
    for (int i = 0; i < n; i++) {
      x[i] = NORMAL.quantile((x[i] - .5) / n);
    }
  }
}
