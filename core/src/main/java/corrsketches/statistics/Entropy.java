package corrsketches.statistics;

import java.util.Arrays;

public class Entropy {

  public static double entropy(int[] data) {
    if (data.length <= 1) {
      return 0.0;
    }

    // Sort data for counting frequencies
    int[] x = Arrays.copyOf(data, data.length);
    Arrays.sort(x);

    // Count the frequency of each unique element in the input data
    int[] frequencies = new int[x.length];
    int nlabels = 0;
    frequencies[0]++;
    for (int i = 1; i < x.length; i++) {
      if (x[i] != x[i - 1]) {
        nlabels++;
      }
      frequencies[nlabels]++;
    }
    nlabels++;

    if (nlabels <= 1) {
      return 0.0;
    }

    // Transform element frequencies into probabilities
    final double[] px = new double[nlabels];
    for (int i = 0; i < nlabels; i++) {
      px[i] = frequencies[i] / (double) data.length;
    }

    // Compute entropy from probabilities
    return entropyFromProbs(px);
  }

  /**
   * Computes the entropy of the probabilities given as input in the array {@param probabilities}.
   *
   * @param probabilities a vector containing probabilities of each unique element of the random
   *     variable X.
   * @return the entropy of probabilities
   */
  public static double entropyFromProbs(double[] probabilities) {
    double e = 0.0;
    for (double p : probabilities) {
      e += p * Math.log(p);
    }
    return -1 * e;
  }
}
