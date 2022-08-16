package corrsketches.correlation;

import static java.lang.Math.log;
import static java.lang.Math.max;
import static smile.math.special.Gamma.digamma;

import corrsketches.statistics.NearestNeighbors1D;
import corrsketches.statistics.NearestNeighbors1D.NearestNeighbor;
import corrsketches.util.Sorting;
import it.unimi.dsi.fastutil.doubles.DoubleArrayList;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Implements computation of mutual information between continuous and discrete variables as
 * described in the paper: Ross, Brian C. "Mutual information between discrete and continuous data
 * sets." PloS one 9, no. 2 (2014): e87357.
 *
 * <p>This class implements the same algorithm from (and is based on) the original Matlab
 * implementation provided by the paper authors.
 */
public class MutualInformationDC {

  public static double mi(int[] d, double[] c) {
    return miNonNegative(d, c, 3);
  }

  public static double mi(int[] d, double[] c, int k) {
    return miNonNegative(d, c, k);
  }

  static double miNonNegative(final int[] d, final double[] c, final int k, final double base) {
    return miNonNegative(d, c, k) / log(base);
  }

  static double miNonNegative(final int[] d, final double[] c, final int k) {
    return max(0, miRaw(d, c, k));
  }

  static double miRaw(
      final int[] discrete, final double[] continuous, final int k, final double base) {
    return miRaw(discrete, continuous, k) / log(base);
  }

  /**
   * Computes the mutual information between the array {@code discrete} and the array {@code
   * continuous} using the estimator described in the paper: Ross, Brian C. "Mutual information
   * between discrete and continuous data sets." PloS one 9, no. 2 (2014): e87357.
   *
   * @param discrete a vector of discrete values represented as integers
   * @param continuous a vector of continuous variables
   * @param k the number k-nearest neighbors
   * @return the mutual information estimate.
   */
  static double miRaw(final int[] discrete, final double[] continuous, final int k) {

    // Make copy to avoid mutating original data
    final int[] d = Arrays.copyOf(discrete, discrete.length);
    final double[] c = Arrays.copyOf(continuous, continuous.length);

    // Sort the data by the continuous variable 'c'
    sort(c, d);

    // Group the continuous data 'c' according to the discrete symbols in 'd'
    List<DoubleArrayList> c_split = groupContinuousByCategories(d, c);
    final int num_d_symbols = c_split.size();

    //
    // Compute the neighbor statistic for each data pair (c, d)
    // using the binned c_split list
    //
    double psi_m_sum = 0;
    double psi_Nd_avg = 0;
    double psi_k_avg = 0;
    final int N = d.length;
    final NearestNeighbor nn = new NearestNeighbor();
    for (final DoubleArrayList split : c_split) {
      int one_k = Math.min(k, split.size() - 1);
      if (one_k > 0) {
        for (int i = 0; i < split.size(); i++) {
          // find the radius of our volume using only those samples with
          // the particular value of the discrete symbol 'd'
          NearestNeighbors1D.kthNearest(split.elements(), split.size(), i, one_k, nn);

          // count the number of total samples within our volume using all samples
          // (all values of 'c')
          final double one_c = split.getDouble(i);
          final double min;
          final double max;
          if (nn.left) {
            min = nn.kthNearest;
            max = one_c + nn.distance;
          } else {
            min = one_c - nn.distance;
            max = nn.kthNearest;
          }
          double m = NearestNeighbors1D.countPointsInRange(c, c.length, min, max);
          if (m < one_k) {
            m = one_k;
          }

          psi_m_sum += digamma(m);
        }
      } else {
        psi_m_sum += digamma(num_d_symbols * 2);
      }

      final double p_d = split.size() / (double) N;
      psi_Nd_avg += p_d * digamma(p_d * N);
      psi_k_avg += p_d * digamma(Math.max(one_k, 1));
    }

    // In the comments below, <> indicates average, and
    // psi is the digamma function described in the paper
    return (digamma(N) // psi(N)
        - psi_Nd_avg // < psi(N_x) >
        + psi_k_avg // < psi(k) >
        - (psi_m_sum / (double) N) // < psi(m) >
    );
  }

  /**
   * Groups the continuous values in array {@code c} according to the discrete values in {@code d}
   * that they are paired with. This method assumes that the input is ordered by the continuous
   * variable {@code c}, and also returns the numerical splits sorted.
   *
   * @param d an array containing discrete values
   * @param c an array containing continuous values
   * @return cSplit: an 'inverted index' containing all the continuous values associated with each
   *     discrete value. The index of the outer list represents a discrete value, and the list
   *     doubles are the list of continuous values associated with the discrete value represented by
   *     the index of the outer list.
   */
  private static List<DoubleArrayList> groupContinuousByCategories(int[] d, double[] c) {
    List<DoubleArrayList> cSplit = new ArrayList<>();
    IntArrayList firstSymbol = new IntArrayList();
    int numSymbols = 0;
    int[] symbolIDs = new int[d.length];

    for (int c1 = 0; c1 < d.length; c1++) {
      // symbol_ID's are integers starting from 0
      symbolIDs[c1] = numSymbols;

      // Find if symbol d[c1] has appeared before, and if so, use its first symbolID (c2)
      // for value in d[c1]
      for (int c2 = 0; c2 < numSymbols; c2++) {
        if (d[c1] == d[firstSymbol.getInt(c2)]) {
          symbolIDs[c1] = c2;
          break;
        }
      }

      if (symbolIDs[c1] >= numSymbols) {
        numSymbols++;
        firstSymbol.add(c1); // record the position where the symbol c1 appeared for the first time
        cSplit.add(new DoubleArrayList());
      }
      // cSplit works as an inverted index: it will record all continuous values in c[] that appear
      // together with the symbol[c1]
      cSplit.get(symbolIDs[c1]).add(c[c1]);
    }
    return cSplit;
  }

  private static void sort(double[] c, int[] d) {
    Sorting.sort(
        new Sorting.Sortable() {
          @Override
          public int compare(int i, int j) {
            // NOTE: The Double.compare() comparator is required to keep compatibility with the
            // original Matlab implementation, which sorts NaN values at the end of the array (which
            // is different from Java's native double comparison operator (<, >) that places NaN
            // values in the beginning).
            return Double.compare(c[i], c[j]);
          }

          @Override
          public void swap(int i, int j) {
            Sorting.swap(d, i, j);
            Sorting.swap(c, i, j);
          }
        },
        0,
        c.length);
  }
}
