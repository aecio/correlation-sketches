package corrsketches.benchmark.distributions;

import static corrsketches.statistics.Stats.toProbabilities;

import java.util.Arrays;
import java.util.Random;

public class MultinomialSampler {

  private final BinomialSampler binomialSampler;
  final double[] probabilities;
  final int n;

  public MultinomialSampler(Random random, final int n, double[] probabilities) {
    this(new BinomialSampler(random), n, probabilities);
  }

  public MultinomialSampler(BinomialSampler binomialSampler, final int n, double[] probabilities) {
    if (probabilities == null || probabilities.length == 0) {
      throw new IllegalArgumentException("Probabilities vector cannot be null or empty");
    }
    this.binomialSampler = binomialSampler;
    this.n = n;
    this.probabilities = toProbabilities(probabilities);
  }

  /**
   * Returns the result of a multinomial sample of n trials. The value of k (the number of possible
   * outcomes) is determined by the number of probabilities passed in.
   *
   * <p>This assumes that `probabilities` sums to 1.0.
   */
  public double[][] sample(int length) {
    final double[][] result = new double[probabilities.length][length];
    final int d = probabilities.length - 1;
    for (int row = 0; row < length; row++) {
      double remainingProb = 1.0;
      int remainingN = n;
      for (int pi = 0; pi < d; pi++) {
        if (probabilities[pi] / remainingProb <= 0) {
          System.out.println("pi = " + pi);
          System.out.println("probabilities = " + Arrays.toString(probabilities));
        }
        final int sample = sampleBinomial(remainingN, probabilities[pi] / remainingProb);
        result[pi][row] = sample;
        remainingN -= sample;
        if (remainingN <= 0) {
          break;
        }
        remainingProb -= probabilities[pi];
      }
      if (remainingN > 0) {
        result[d][row] = remainingN;
      }
    }
    return result;
  }

  private int sampleBinomial(int n, double p) {
    if (p >= 1.0) {
      return n;
    }
    if (n <= 0) {
      return 0;
    }
    return binomialSampler.binomialSWT(n, p);
    //    return binomialSampler.sampleNaive(n, p);
  }
}
