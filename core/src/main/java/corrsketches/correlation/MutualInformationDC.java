package corrsketches.correlation;

import static java.lang.Math.log;
import static smile.math.special.Gamma.digamma;

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
    return mi(d, c, 3, Math.exp(1));
  }

  public static double mi(int[] d, double[] c, int k) {
    return mi(d, c, k, Math.exp(1));
  }

  /**
   * Computes the mutual information between the array {@param discrete} and the array {@param
   * continuous} using the estimator described in the paper: Ross, Brian C. "Mutual information
   * between discrete and continuous data sets." PloS one 9, no. 2 (2014): e87357.
   *
   * @param discrete a vector of discrete values represented as integers
   * @param continuous a vector of continuous variables
   * @param k the number k-nearest neighbors
   * @param base the base of the log
   * @return the mutual information estimate.
   */
  public static double mi(
      final int[] discrete, final double[] continuous, final int k, final double base) {

    // Make copy to avoid mutating original data
    final int[] d = Arrays.copyOf(discrete, discrete.length);
    final double[] c = Arrays.copyOf(continuous, continuous.length);

    // Sort the data by the continuous variable 'c'
    sort(d, c);

    //
    // Bin the continuous data 'c' according to the discrete symbols 'd'
    //
    List<DoubleArrayList> c_split = binContinuousData(d, c);
    final int num_d_symbols = c_split.size();

    //
    // Compute the neighbor statistic for each data pair (c, d)
    // using the binned c_split list
    //
    double m_tot = 0;
    double av_psi_Nd = 0;
    double psi_ks = 0;

    final int N = d.length;

    for (final DoubleArrayList split : c_split) {
      int one_k = Math.min(k, split.size() - 1);
      if (one_k > 0) {
        for (int pivot = 1; pivot <= split.size(); pivot++) {
          // find the radius of our volume using only those samples with
          // the particular value of the discrete symbol 'd'
          int leftNeighbor = pivot;
          int rightNeighbor = pivot;
          int theNeighbor = -1; // initial value not used, but needed to make compiler happy

          double one_c = split.getDouble(pivot - 1);

          for (int ck = 1; ck <= one_k; ck++) {
            if (leftNeighbor == 1) {
              rightNeighbor = rightNeighbor + 1;
              theNeighbor = rightNeighbor;
            } else if (rightNeighbor == split.size()) {
              leftNeighbor = leftNeighbor - 1;
              theNeighbor = leftNeighbor;
            } else if (Math.abs(split.getDouble(leftNeighbor - 1 - 1) - one_c)
                < Math.abs(split.getDouble(rightNeighbor + 1 - 1) - one_c)) {
              leftNeighbor = leftNeighbor - 1;
              theNeighbor = leftNeighbor;
            } else {
              rightNeighbor = rightNeighbor + 1;
              theNeighbor = rightNeighbor;
            }
          }
          double distance_to_neighbor = Math.abs(split.getDouble(theNeighbor - 1) - one_c);

          //
          // count the number of total samples within our volume using all samples
          // (all values of 'd')
          //
          double m;
          if (theNeighbor == leftNeighbor) {
            final double target = split.getDouble(leftNeighbor - 1);
            m = Math.floor(findPoint(c, one_c + distance_to_neighbor) - findPoint(c, target));
          } else {
            final double target = split.getDouble(rightNeighbor - 1);
            m = Math.floor(findPoint(c, target) - findPoint(c, one_c - distance_to_neighbor));
          }
          if (m < one_k) {
            m = one_k;
          }

          m_tot = m_tot + digamma(m);
        }
      } else {
        m_tot = m_tot + digamma(num_d_symbols * 2);
      }

      final double p_d = split.size() / (double) N;
      av_psi_Nd += p_d * digamma(p_d * N);
      psi_ks += p_d * digamma(Math.max(one_k, 1));
    }

    // In the comments below, <> indicates average, and
    // psi is the digamma function described in the paper
    return (digamma(N) // psi(N)
            - av_psi_Nd // < psi(N_x) >
            + psi_ks // < psi(k) >
            - (m_tot / (double) N) // < psi(m) >
        )
        / log(base);
  }

  /**
   * Groups the continuous values in array {@param c} according to the discrete values in {@param d}
   * that they are paired with.
   *
   * @param d an array containing discrete values
   * @param c an array containing continuous values
   * @return cSplit: an 'inverted index' containing all the continuous values associated with each
   *     discrete value. The index of the outer list represents a discrete value, and the list
   *     doubles are the list of continuous values associated with the discrete value represented by
   *     the index of the outer list.
   */
  private static List<DoubleArrayList> binContinuousData(int[] d, double[] c) {
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

  /** findpt() finds the data point whose value for the continuous variable is closest to 'c'. */
  static double findPoint(double[] c, double target) {
    int left = 1;
    int right = c.length;

    if (target < c[left - 1]) {
      return 0.5;
    } else if (target > c[right - 1]) {
      return right + 0.5;
    }

    double pt = -1;
    while (left != right) {
      pt = (left + right) / 2;
      if (c[(int) pt - 1] < target) {
        left = (int) pt;
      } else {
        right = (int) pt;
      }
      if (left + 1 == right) {
        if (c[left - 1] == target) {
          pt = left;
        } else if (c[right - 1] == target) {
          pt = right;
        } else {
          pt = (right + left) / 2d;
        }
        break;
      }
    }
    return pt;
  }

  private static void sort(int[] d, double[] c) {
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
