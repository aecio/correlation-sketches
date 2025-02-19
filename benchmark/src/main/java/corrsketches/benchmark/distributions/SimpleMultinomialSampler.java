package corrsketches.benchmark.distributions;

import static corrsketches.statistics.Stats.sum;

import java.util.Random;

/**
 * Get samples from a multinomial distribution.
 *
 * <p>Based on: https://github.com/tedunderwood/LDA/blob/master/Multinomial.java
 *
 * <p>NOTE: Do not use this code in production unless you have thoroughly tested it and are
 * comfortable that you know what it does!
 */
public class SimpleMultinomialSampler {

  private final Random rng;
  private final double[] probabilities;

  // NOTE: Ideally should pass in a ThreadLocalRandom.
  public SimpleMultinomialSampler(final Random random, final double[] probabilities) {
    this.rng = random;
    // Normalize all the passed in "probabilities" so that they sum to 1.0.
    this.probabilities = normalizeProbabilitiesToCumulative(probabilities);
  }

  /**
   * Returns the result of a multinomial sample of n trials. The value of k (the number of possible
   * outcomes) is determined by the number of probabilities passed into the constructor.
   */
  public int[] multinomialSample(final int n) {
    // The length of `probabilities` is the number of possible outcomes.
    final int[] result = new int[this.probabilities.length];

    // Get the result of each trial and increment the count for that outcome.
    for (int i = 0; i < n; i++) {
      result[multinomialTrial()]++;
    }

    return result;
  }

  public double[][] sampleVector(final int n, final int length) {
    final double[][] result = new double[this.probabilities.length][length];
    for (int i = 0; i < length; i++) {
      for (int j = 0; j < n; j++) {
        result[multinomialTrial()][i]++;
      }
    }
    return result;
  }

  /**
   * The `probabilities` field is an array of "cumulative" probabilities. The first element has the
   * value p1, the second has p1 + p2, the third has p1 + p2 + p3, etc. By definition, the last bin
   * should have a value of 1.0.
   */
  public int multinomialTrial() {
    double sample = rng.nextDouble(); // Between [0, 1)
    for (int i = 0; i < this.probabilities.length; ++i) {
      // Find the first bucket whose upper bound is above the sampled value.
      if (sample < this.probabilities[i]) {
        return i;
      }
    }
    // Catch-all return statement to ensure code compiles.
    return this.probabilities.length - 1;
  }

  /**
   * Given an array of raw probabilities, this will transform the values in place in the the
   * following manner: 1. The sum of the values will be computed. 2. Each value will be divided by
   * the sum, normalizing them so that the sum is roughly 1.0. 3. The values will be converted into
   * cumulative values.
   *
   * <p>Example: Input is: [0.5, 0.5, 1.0] 1. Sum is 2.0 2. After normalization: [0.25, 0.25, 0.5]
   * 3. After converting to cumulative values: [0.25, 0.5, 1.0]
   *
   * <p>The form in (3) is useful for converting a uniformly-sampled value between [0, 1) into a
   * multinomial sample, because the values now represent the upper-bounds of the "range" between
   * [0, 1) that the represent the probability of that outcome. Thus, given a uniformly-sampled
   * value between [0, 1), we just need to find the first/lowest bin whose upper-bound is more than
   * the sampled value.
   */
  public static double[] normalizeProbabilitiesToCumulative(final double[] probabilities) {
    if (probabilities == null || probabilities.length < 2) {
      throw new IllegalArgumentException("probabilities must have more than one value");
    }

    double sum = sum(probabilities);
    double cumulative = 0.0;
    final double[] distribution = new double[probabilities.length];
    for (int i = 0; i < probabilities.length; i++) {
      cumulative += probabilities[i];
      distribution[i] = cumulative / sum;
    }

    // To ensure the right-most bin is always 1.0.
    distribution[distribution.length - 1] = 1.0;

    return distribution;
  }
}
