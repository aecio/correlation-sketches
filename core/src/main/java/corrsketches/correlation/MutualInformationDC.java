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

  /**
   * Computes the mutual information between the array {@param discrete} and the array {@param
   * continuous} using the estimator described in the paper: Ross, Brian C. "Mutual information
   * between discrete and continuous data * sets." PloS one 9, no. 2 (2014): e87357.
   *
   * @param discrete a vector of discrete values represented as integers
   * @param continuous a vector of continuous variables
   * @param k the number k-nearest neighbors
   * @param base the base of the log
   * @return the mutual information estimate.
   */
  public static double mi(
      final int[] discrete, final double[] continuous, final int k, final double base) {

    final int[] d = Arrays.copyOf(discrete, discrete.length);
    final double[] c = Arrays.copyOf(continuous, continuous.length);

    // Init variables
    IntArrayList first_symbol = new IntArrayList(); // first_symbol = [];
    int[] symbol_IDs = new int[d.length]; // symbol_IDs = zeros(1, length(d));
    List<DoubleArrayList> c_split = new ArrayList<>(); // c_split = {};
    List<IntArrayList> cs_indices = new ArrayList<>(); // cs_indices = {};
    int num_d_symbols = 0; // num_d_symbols = 0;

    // Sort the data by the continuous variable 'c'
    // [c, c_idx] = sort(c);
    // d = d(c_idx);
    Sorting.sort(
        new Sorting.Sortable() {
          /** Comparator for arrays of doubles that places NaN's at the end of the array. */
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
            int di = d[i];
            d[i] = d[j];
            d[j] = di;
            double ci = c[i];
            c[i] = c[j];
            c[j] = ci;
          }
        },
        0,
        c.length);

    //
    // Bin the continuous data 'c' according to the discrete symbols 'd'
    //

    // for c1 = 1:length(d)
    for (int c1 = 1; c1 <= d.length; c1++) {
      // symbol_IDs(c1) = num_d_symbols+1;
      symbol_IDs[c1 - 1] = num_d_symbols + 1;
      // for c2 = 1:num_d_symbols
      for (int c2 = 1; c2 <= num_d_symbols; c2++) {
        // if d(c1) == d(first_symbol(c2))
        if (d[c1 - 1] == d[first_symbol.getInt(c2 - 1) - 1]) {
          // symbol_IDs(c1) = c2;
          symbol_IDs[c1 - 1] = c2;
          break;
        }
      }

      // if symbol_IDs(c1) > num_d_symbols
      if (symbol_IDs[c1 - 1] > num_d_symbols) {
        // num_d_symbols = num_d_symbols+1;
        num_d_symbols++;
        // first_symbol(num_d_symbols) = c1;
        first_symbol.add(c1);
        // c_split{num_d_symbols} = [];
        c_split.add(new DoubleArrayList());
        // cs_indices{num_d_symbols} = [];
        cs_indices.add(new IntArrayList());
      }
      // c_split{symbol_IDs(c1)} = [ c_split{symbol_IDs(c1)} c(c1) ];
      c_split.get(symbol_IDs[c1 - 1] - 1).add(c[c1 - 1]);
      // cs_indices{symbol_IDs(c1)} = [ cs_indices{symbol_IDs(c1)} c1 ];
      cs_indices.get(symbol_IDs[c1 - 1] - 1).add(c1);
    }

    //
    // Compute the neighbor statistic for each data pair (c, d)
    // using the binned c_split list
    //
    double m_tot = 0;
    double av_psi_Nd = 0;
    double[] V = new double[d.length];
    double psi_ks = 0;

    // for c_bin = 1:num_d_symbols
    for (int c_bin = 1; c_bin <= num_d_symbols; c_bin++) {
      // one_k = min(k, size(c_split{c_bin}, 2)-1);
      int one_k = Math.min(k, c_split.get(c_bin - 1).size() - 1);
      // if one_k > 0
      if (one_k > 0) {
        // for pivot = 1:length(c_split{c_bin})
        for (int pivot = 1; pivot <= c_split.get(c_bin - 1).size(); pivot++) {
          // find the radius of our volume using only those samples with
          // the particular value of the discrete symbol 'd'
          int left_neighbor = pivot;
          int right_neighbor = pivot;
          int the_neighbor = -1; // initial value not used

          // one_c = c_split{c_bin}(pivot);
          double one_c = c_split.get(c_bin - 1).getDouble(pivot - 1);

          // for ck = 1:one_k
          for (int ck = 1; ck <= one_k; ck++) {
            if (left_neighbor == 1) {
              right_neighbor = right_neighbor + 1;
              the_neighbor = right_neighbor;
            } else if (right_neighbor == c_split.get(c_bin - 1).size()) {
              left_neighbor = left_neighbor - 1;
              the_neighbor = left_neighbor;
            } else if (Math.abs(c_split.get(c_bin - 1).getDouble(left_neighbor - 1 - 1) - one_c)
                < Math.abs(c_split.get(c_bin - 1).getDouble(right_neighbor + 1 - 1) - one_c)) {
              left_neighbor = left_neighbor - 1;
              the_neighbor = left_neighbor;
            } else {
              right_neighbor = right_neighbor + 1;
              the_neighbor = right_neighbor;
            }
          }
          DoubleArrayList c_bin_split = c_split.get(c_bin - 1);
          double distance_to_neighbor = Math.abs(c_bin_split.getDouble(the_neighbor - 1) - one_c);

          //
          // count the number of total samples within our volume using all samples (all values of
          // 'd')
          //
          double m;
          if (the_neighbor == left_neighbor) {
            final double target = c_split.get(c_bin - 1).getDouble(left_neighbor - 1);
            m = Math.floor(findpt(c, one_c + distance_to_neighbor) - findpt(c, target));
          } else {
            final double target = c_split.get(c_bin - 1).getDouble(right_neighbor - 1);
            m = Math.floor(findpt(c, target) - findpt(c, one_c - distance_to_neighbor));
          }
          if (m < one_k) {
            m = one_k;
          }

          // m_tot = m_tot + psi(m);
          m_tot = m_tot + digamma(m);
          // V(cs_indices{c_bin}(pivot)) = 2 * distance_to_neighbor;
          V[cs_indices.get(c_bin - 1).getInt(pivot - 1) - 1] = 2 * distance_to_neighbor;
        }
      } else {
        // m_tot = m_tot + psi(num_d_symbols*2);
        m_tot = m_tot + digamma(num_d_symbols * 2);
        // V(cs_indices{c_bin}(1)) = 2 * (c(end) - c(1));
        V[cs_indices.get(c_bin - 1).getInt(1 - 1) - 1] = 2 * (c[c.length - 1] - c[1 - 1]);
      }

      double p_d = c_split.get(c_bin - 1).size() / (double) d.length;
      av_psi_Nd = av_psi_Nd + p_d * digamma(p_d * d.length);
      psi_ks = psi_ks + p_d * digamma(Math.max(one_k, 1));
    }

    double[] dps = new double[num_d_symbols];
    for (int c_bin = 1; c_bin <= num_d_symbols; c_bin++) {
      // dps(c_bin) = length(c_split{c_bin})/length(d);
      dps[c_bin - 1] = c_split.get(c_bin - 1).size() / (double) d.length;
    }

    // f = (psi(length(d)) - av_psi_Nd + psi_ks - m_tot/length(d)) / log(base);
    double f = (digamma(d.length) - av_psi_Nd + psi_ks - (m_tot / (double) d.length)) / log(base);
    return f;
  }

  /**
   * findpt() finds the data point whose the value for the continuous variable is closest to 'c'.
   */
  static double findpt(double[] c, double target) {
    double left = 1;
    double right = c.length;
    double pt = -1;

    if (target < c[(int) left - 1]) {
      pt = 0.5;
      return pt;
    } else if (target > c[(int) right - 1]) {
      pt = right + 0.5;
      return pt;
    }

    while (left != right) {
      pt = Math.floor((left + right) / 2d);
      if (c[(int) pt - 1] < target) {
        left = (int) pt;
      } else {
        right = (int) pt;
      }
      if (left + 1 == right) {
        if (c[(int) left - 1] == target) {
          pt = left;
        } else if (c[(int) right - 1] == target) {
          pt = right;
        } else {
          pt = (right + left) / 2;
        }
        break;
      }
    }
    return pt;
  }
}
