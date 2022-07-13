package corrsketches.correlation;

import static corrsketches.statistics.Stats.*;

import corrsketches.util.RandomArrays;
import corrsketches.util.Sorting;

/** Computes Chatterjee's cross-rank correlation coefficient. */
public class ChatterjeeCorrelation {

  /** Computes the Chatterjee's correlation coefficient for two vectors. */
  public static Estimate estimate(double[] x, double[] y) {
    double r = coefficient(x, y);
    return new Estimate(r, x.length);
  }

  /** Computes the Chatterjee's correlation coefficient for two vectors. */
  public static double coefficient(double[] x, double[] y) {
    final int n = x.length;
    final double[] a = x.clone();
    final double[] b = y.clone();

    final int[] tieBreaker = RandomArrays.randIntUniform(x.length);

    //
    // Sort both vectors according to y's order, then compute the number of j s.t. y[j] <= y[i],
    // divided by n, that will be used in the numerator.
    //
    sort(b, a, tieBreaker);
    rank(b, TiesMethod.MAX); // rank() computes the rank in-place
    for (int i = 0; i < n; i++) {
      b[i] = b[i] / n;
    }

    //
    // Compute the number of j s.t. y[j] >= y[i] used in the denominator.
    //
    final int lastIdx = b.length - 1;
    final double[] gr = new double[b.length];
    // Given that b is already sorted, we just need to invert the array order to get a sorted array
    // with the additive inverse of b.
    for (int i = 0; i < gr.length; i++) {
      gr[i] = -b[lastIdx - i];
    }
    rank(gr, TiesMethod.MAX);
    for (int i = 0; i < n; i++) {
      final double g = gr[i] / n;
      gr[i] = g * (1 - g);
    }

    // sort according to x to compute the sum of absolute differences for the numerator
    sort(a, b, tieBreaker);

    double A1 = sumAbsDiff(b) / (2. * n);
    double CU = mean(gr);
    double xi = 1.0 - A1 / CU;
    return xi;
  }

  private static double[] additiveInverse(double[] y) {
    double[] gr = new double[y.length];
    for (int i = 0; i < gr.length; i++) {
      gr[i] = -y[i];
    }
    return gr;
  }

  protected static double sumAbsDiff(double[] x) {
    double sum = 0.0;
    for (int i = 0; i < x.length - 1; i++) {
      sum += Math.abs(x[i + 1] - x[i]);
    }
    return sum;
  }

  private static void sort(double[] x, double[] y, int[] tieBreaker) {
    Sorting.sort(
        new Sorting.Sortable() {
          @Override
          public int compare(int i, int j) {
            final int v = Double.compare(x[i], x[j]);
            if (v == 0) {
              return Integer.compare(tieBreaker[i], tieBreaker[j]);
            } else {
              return v;
            }
          }

          @Override
          public void swap(int i, int j) {
            Sorting.swap(x, i, j);
            Sorting.swap(y, i, j);
            Sorting.swap(tieBreaker, i, j);
          }
        },
        0,
        x.length);
  }
}
