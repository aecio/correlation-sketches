package corrsketches.correlation;

import static com.google.common.base.Preconditions.checkArgument;

import corrsketches.Column;
import it.unimi.dsi.fastutil.ints.Int2IntMap;
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;

public class MutualInformation {

  public static Estimate estimateMi(final double[] x, final double[] y) {
    return new Estimate(MutualInformation.ofCategorical(x, y).value, x.length);
  }

  public static Estimate estimateNmiSqrt(double[] x, double[] y) {
    return new Estimate(MutualInformation.ofCategorical(x, y).nmiSqrt(), x.length);
  }

  public static Estimate estimateNmiMax(double[] x, double[] y) {
    return new Estimate(MutualInformation.ofCategorical(x, y).nmiMax(), x.length);
  }

  public static Estimate estimateNmiMin(double[] x, double[] y) {
    return new Estimate(MutualInformation.ofCategorical(x, y).nmiMin(), x.length);
  }

  public static MI ofCategorical(final Column x, final Column y) {
    return ofCategorical(x.values, y.values);
  }

  /**
   * Computes the (Normalized) Mutual Information (MI) between the elements of {@param x} and
   * {@param y}. This functions assumes that the values in {@param x} and {@param y} are integers
   * that identify categorical data entries and explicitly casts their double values to ints.
   */
  public static MI ofCategorical(final double[] x, final double[] y) {
    checkArgument(x.length == y.length, "x and y must have same size");
    int[] xi = castToIntArray(x);
    int[] yi = castToIntArray(y);
    return ofCategorical(xi, yi);
  }

  public static MI ofCategorical(int[] x, int[] y) {
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

    return new MI(mi, n, entropy(px), entropy(py), xlabelLength, ylabelLength);
  }

  /**
   * Computes the entropy of the probabilities given as input in the array {@param probs}.
   *
   * @param probs a vector containing probabilities of the random variable X.
   * @return the entropy of probs
   */
  private static double entropy(double[] probs) {
    double e = 0.0;
    for (double p : probs) {
      e += p * Math.log(p);
    }
    return -1 * e;
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

  private static int[] castToIntArray(double[] x) {
    int[] xi = new int[x.length];
    for (int i = 0; i < x.length; i++) {
      xi[i] = (int) x[i];
    }
    return xi;
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

  public static final class MI extends Estimate {

    // the MI is stored in super class, here we store additional data
    public final double ex; // entropy of x
    public final double ey; // entropy of y
    public final int nx; // cardinality of x
    public final int ny; // cardinality of y

    public MI(double mi, int sampleSize) {
      this(mi, sampleSize, -1, -1, -1, -1);
    }

    public MI(double mi, int sampleSize, double ex, double ey, int nx, int ny) {
      super(mi, sampleSize);
      this.ex = ex;
      this.ey = ey;
      this.nx = nx;
      this.ny = ny;
    }

    public double nmiMax() {
      return value / Math.max(ex, ey);
    }

    public double nmiMin() {
      return value / Math.min(ex, ey);
    }

    public double nmiSqrt() {
      return value / Math.sqrt(ex * ey);
    }

    public double infoGainRatioX() {
      return value / ex;
    }

    public double infoGainRatioY() {
      return value / ey;
    }
  }
}
