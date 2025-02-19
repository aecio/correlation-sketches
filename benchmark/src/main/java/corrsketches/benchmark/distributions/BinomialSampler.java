package corrsketches.benchmark.distributions;

import java.util.Random;

/**
 * Get samples from a binomial distribution.
 *
 * <p>NOTE: Do not use this code in production unless you have thoroughly tested it and are
 * comfortable that you know what it does!
 */
public class BinomialSampler {

  private final Random random;

  public BinomialSampler(final Random random) {
    this.random = random;
  }

  public int sampleNaive(int n, double p) {
    checkArguments(n, p);
    return sampleNaive(random, n, p);
  }

  /**
   * Returns a sample from a Binomial Distribution with parameters n (number of trials) and p
   * (probability of success of an individual trial).
   *
   * <p>The sample is the number of successes observed.
   */
  public static int sampleNaive(Random random, final int n, final double p) {
    int successes = 0;
    for (int i = 0; i < n; i++) {
      if (random.nextDouble() < p) {
        // Given that nextDouble() returns a uniformly-distributed random value in [0, 1),
        // a success occurs with probability approximately equal to p
        successes++;
      }
    }
    return successes;
  }

  public int binomialSWT(int n, double p) {
    checkArguments(n, p);
    if (p <= 0.5) {
      final double q = 1.0 - p;
      return binomialSWT(random, n, q);
    } else {
      return n - binomialSWT(random, n, p);
    }
  }

  /**
   * Generates a random sample from a Binomial distribution with parameters n and p. This method
   * uses a variation of the Second Waiting Time (SWT) method as described by Luc Devroye in pages
   * 522 and 526 of his book "Non-Uniform Random Variate Generation" (book PDF:
   * www.eirene.de/Devroye.pdf).
   *
   * @param random the random number generator
   * @param n number of binomial trials
   * @param q (1-p) where p is the success probability
   * @return
   */
  public static int binomialSWT(Random random, int n, double q) {
    final double logQ = Math.log(q);
    int x = 0;
    double sum = Math.log(random.nextDouble()) / (n - x);
    while (sum >= logQ) {
      x++;
      sum += Math.log(random.nextDouble()) / (n - x);
    }
    return x;
  }

  private static void checkArguments(int n, double p) {
    if (n <= 0) {
      throw new IllegalArgumentException(
          "The parameter n must be positive, but n was <= 0. n=" + n);
    }
    if (p >= 1) {
      throw new IllegalArgumentException(
          "Probability p must be in range (0,1), but p was >= 1. p=" + p);
    }
    if (p <= 0) {
      throw new IllegalArgumentException(
          "Probability p must be in range (0,1), but p was <= 1. p=" + p);
    }
  }
}
