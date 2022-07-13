package corrsketches.correlation;

import static com.google.common.base.Preconditions.checkArgument;

import java.util.Arrays;

public class MutualInformation {

  public static Estimate estimate(final double[] x, final double[] y) {
    checkArgument(x.length == y.length, "x and y must have same size");
    int n = x.length;
    int[] xi = new int[n];
    for (int i = 0; i < n; i++) {
      xi[i] = (int) x[i];
    }
    int[] yi = new int[n];
    for (int i = 0; i < n; i++) {
      yi[i] = (int) y[i];
    }
    return new Estimate(score(xi, yi), n);
  }

  public static double score(int[] x, int[] y) {
    checkArgument(x.length == y.length, "x and y must have same size");

    final int n = x.length;
    final int[] xlabels = Arrays.stream(x).distinct().toArray();
    final int[] ylabels = Arrays.stream(x).distinct().toArray();

    int[][] cooMatrix = coOccurrenceMatrix(x, y, n, xlabels, ylabels);

    // Now compute the marginals using the co-occurrence matrix
    final int[] xSum = new int[xlabels.length];
    final int[] ySum = new int[ylabels.length];
    for (int i = 0; i < xlabels.length; i++) {
      for (int j = 0; j < ylabels.length; j++) {
        xSum[i] += cooMatrix[i][j];
        ySum[j] += cooMatrix[i][j];
      }
    }

    // transform marginals into probabilities
    final double[] px = new double[xlabels.length];
    for (int i = 0; i < xlabels.length; i++) {
      px[i] = xSum[i] / (double) n;
    }
    final double[] py = new double[ylabels.length];
    for (int i = 0; i < ylabels.length; i++) {
      py[i] = ySum[i] / (double) n;
    }

    // compute the mutual information
    double mi = 0.0;
    for (int i = 0; i < px.length; i++) {
      for (int j = 0; j < py.length; j++) {
        if (cooMatrix[i][j] > 0) {
          final double p = cooMatrix[i][j] / (double) n;
          mi += p * Math.log(p / (px[i] * py[j]));
        }
      }
    }

    return mi;
  }

  /**
   * Computes the co-occurrence matrix by doing one pass in the original vectors for each pair of
   * labels. The algorithm assumes that the number of labels is small, and doing {@code
   * xlabels.length * ylabels.length} passes in the original arrays does not cause a performance
   * problem.
   */
  private static int[][] coOccurrenceMatrix(int[] x, int[] y, int n, int[] xlabels, int[] ylabels) {
    int[][] cooMatrix = new int[xlabels.length][ylabels.length];
    for (int i = 0; i < xlabels.length; i++) {
      for (int j = 0; j < ylabels.length; j++) {
        // computes co-occurrences for the labels i and j
        int occurrences = 0;
        for (int k = 0; k < n; k++) {
          if (xlabels[i] == x[k] && ylabels[j] == y[k]) {
            occurrences++;
          }
        }
        cooMatrix[i][j] = occurrences;
      }
    }
    return cooMatrix;
  }
}
