package corrsketches.correlation;

import static com.google.common.base.Preconditions.checkArgument;

import it.unimi.dsi.fastutil.ints.Int2IntMap;
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;


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
    int[][] cooMatrix = coOccurrenceMatrix(x, y, n);
    final int xlabelLength = cooMatrix.length;
    final int ylabelLength = cooMatrix[0].length;

    // Now compute the marginals using the co-occurrence matrix
    final int[] xSum = new int[xlabelLength];
    final int[] ySum = new int[ylabelLength];

    for (int i = 0; i < xlabelLength; i++) {
      for (int j = 0; j < ylabelLength; j++) {
        xSum[i] += cooMatrix[i][j];
        ySum[j] += cooMatrix[i][j];
      }
    }

    // transform marginals into probabilities
    final double[] px = new double[xlabelLength];
    for (int i = 0; i < xlabelLength; i++) {
      px[i] = xSum[i] / (double) n;
    }
    final double[] py = new double[ylabelLength];
    for (int i = 0; i < ylabelLength; i++) {
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

  /** Computes the co-occurrence matrix. */
  protected static int[][] coOccurrenceMatrix(int[] x, int[] y, int n) {
    Int2IntMap xmap = createValueToIndexMapping(x);
    Int2IntMap ymap = createValueToIndexMapping(y);
    final int[][] cooMatrix = new int[xmap.size()][ymap.size()];
    for (int i = 0; i < n; i++) {
      int xidx = xmap.get(x[i]);
      int yidx = ymap.get(y[i]);
      cooMatrix[xidx][yidx]++;
    }
    return cooMatrix;
  }

  private static Int2IntMap createValueToIndexMapping(int[] x) {
    Int2IntMap indexMap = new Int2IntOpenHashMap();
    int xLabelsLen = 0;
    for (int i = 0; i < x.length; i++) {
      if (!indexMap.containsKey(x[i])) {
        indexMap.put(x[i], xLabelsLen);
        xLabelsLen++;
      }
    }
    return indexMap;
  }
}
