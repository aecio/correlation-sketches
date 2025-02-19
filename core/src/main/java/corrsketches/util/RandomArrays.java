package corrsketches.util;

import corrsketches.statistics.Stats;
import java.util.Arrays;
import java.util.Random;

public class RandomArrays {

  /**
   * Generates an array of size {@code length} with random values that follow a uniform
   * distribution.
   *
   * @param length the size of the output vector
   * @return a vector with random data uniformly distributed
   */
  public static int[] randIntUniform(int length) {
    return randIntUniform(length, new Random());
  }

  /**
   * Generates an array of size {@code length} with random values that follow a uniform
   * distribution.
   *
   * @param length the size of the output vector
   * @param rng the random number generator
   * @return a vector with random data uniformly distributed
   */
  public static int[] randIntUniform(int length, final Random rng) {
    final int[] data = new int[length];
    for (int i = 0; i < data.length; i++) {
      data[i] = rng.nextInt();
    }
    return data;
  }

  /**
   * Generates an array of size {@code length} with random values that follow a standard normal
   * distribution, i.e., a Gaussian distribution with mean 0.0 and standard deviation 1.0.
   *
   * @param length the size of the output vector
   * @return a vector with random data normally distributed
   */
  public static double[] randDoubleStdNormal(int length) {
    return randDoubleStdNormal(length, new Random());
  }

  /**
   * Generates an array of size {@code length} with random values that follow a standard normal
   * distribution, i.e., a Gaussian distribution with mean 0.0 and standard deviation 1.0.
   *
   * @param length the size of the output vector
   * @param rng the random number generator
   * @return a vector with random data normally distributed
   */
  public static double[] randDoubleStdNormal(int length, Random rng) {
    final double[] data = new double[length];
    for (int i = 0; i < data.length; i++) {
      data[i] = rng.nextGaussian();
    }
    return data;
  }

  /**
   * Generates an array of size {@code length} with random values that follow a Uniform distribution
   * with values in the range [0, 1].
   *
   * @param length the size of the output vector
   * @return a vector with random data uniformly distributed
   */
  public static double[] randDoubleUniform(int length) {
    return randDoubleUniform(length, new Random());
  }

  /**
   * Generates an array of size {@code length} with random values that follow a Uniform distribution
   * with values in the range [0, 1].
   *
   * @param length the size of the output vector
   * @param rng the random number generator
   * @return a vector with random data uniformly distributed
   */
  public static double[] randDoubleUniform(int length, Random rng) {
    final double[] data = new double[length];
    for (int i = 0; i < data.length; i++) {
      data[i] = rng.nextDouble();
    }
    return data;
  }

  /**
   * Generates an array of size {@code length} with random values that follow an Exponential
   * distribution with parameter {@param lambda}.
   *
   * @param length the size of the output vector
   * @param lambda the lambda parameter of the exponential distribution
   * @return a vector with random data exponentially distributed
   */
  public static double[] randDoubleExponential(int length, double lambda) {
    return randDoubleExponential(length, lambda, new Random());
  }

  /**
   * Generates an array of size {@code length} with random values that follow an Exponential
   * distribution with parameter {@param lambda}.
   *
   * @param length the size of the output vector
   * @param lambda the lambda parameter of the exponential distribution
   * @param rng the random number generator
   * @return a vector with random data exponentially distributed
   */
  public static double[] randDoubleExponential(int length, double lambda, Random rng) {
    final double[] data = new double[length];
    for (int i = 0; i < data.length; i++) {
      data[i] = -1 / lambda * Math.log(rng.nextDouble());
    }
    return data;
  }

  /**
   * Generates an array of size {@code length} with random values that follow a Rademacher
   * distribution, i.e., where a random variable X has a 50% chance of being +1 and a 50% chance of
   * being -1.
   *
   * @param length the size of the output vector
   * @return a vector with random data that follows the Rademacher distribution
   */
  public static double[] randDoubleRademacher(int length) {
    return randDoubleRademacher(length, new Random());
  }

  /**
   * Generates an array of size {@code length} with random values that follow a Rademacher
   * distribution, i.e., where a random variable X has a 50% chance of being +1 and a 50% chance of
   * being -1.
   *
   * @param length the size of the output vector
   * @param rng the random number generator
   * @return a vector with random data that follows the Rademacher distribution
   */
  public static double[] randDoubleRademacher(int length, Random rng) {
    final double[] data = new double[length];
    for (int i = 0; i < data.length; i++) {
      data[i] = rng.nextDouble() < 0.5 ? -1 : 1;
    }
    return data;
  }

  public static CI percentiles(long[] data, double alpha) {
    double[] y = new double[data.length];
    for (int i = 0; i < data.length; i++) {
      y[i] = data[i];
    }
    return percentiles(y, alpha);
  }

  public static CI percentiles(double[] data, double alpha) {
    Arrays.sort(data);
    double percentile = alpha / 2.0;
    int idxLb = (int) Math.ceil((percentile) * data.length);
    int idxUb = (int) Math.ceil((1. - percentile) * data.length);
    double lb = data[idxLb - 1];
    double ub = data[idxUb - 1];
    final double mean = Stats.mean(data);
    return new CI(mean, lb, ub);
  }

  public static class CI {

    public final double mean;
    public final double ub;
    public final double lb;

    public CI(double mean, double lb, double ub) {
      this.mean = mean;
      this.lb = lb;
      this.ub = ub;
    }
  }
}
