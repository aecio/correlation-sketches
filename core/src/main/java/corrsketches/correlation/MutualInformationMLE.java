package corrsketches.correlation;

import static com.google.common.base.Preconditions.checkArgument;

import it.unimi.dsi.fastutil.ints.Int2IntMap;
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;

public class MutualInformationMLE {

  public static MIEstimate mi(int[] x, int[] y) {
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

    return new MIEstimate(mi, n, px, py);
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

  private static Int2IntMap createValueToIndexMapping(int[] labels) {
    Int2IntMap indexMap = new Int2IntOpenHashMap();
    int xLabelsLen = 0;
    for (int value : labels) {
      if (!indexMap.containsKey(value)) {
        indexMap.put(value, xLabelsLen);
        xLabelsLen++;
      }
    }
    return indexMap;
  }
}
