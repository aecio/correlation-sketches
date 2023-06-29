package corrsketches.benchmark.utils;

import it.unimi.dsi.fastutil.doubles.DoubleArrayList;
import java.math.BigInteger;

public class LogFactorial {

  static DoubleArrayList cache;
  static BigInteger fac;

  static {
    int initialCacheSize = 1024;
    cache = new DoubleArrayList(initialCacheSize);
    fac = BigInteger.valueOf(1); // fac(0) = 1
    cache.add(BigMath.logBigInteger(fac));
    for (int i = 1; i <= initialCacheSize; i++) {
      fac = fac.multiply(BigInteger.valueOf(i)); // fac(n) = n * (n-1)
      cache.add(BigMath.logBigInteger(fac));
    }
  }

  /**
   * Natural logarithm of the factorial of the input parameter {@code n}.
   *
   * @param n the integer for which to compute the factorial
   * @return log(factorial(n))
   */
  public static double logOfFactorial(int n) {
    if (n < 0) {
      throw new IllegalArgumentException("Factorial not defined for negative numbers.");
    }
    if (n >= cache.size()) {
      expandFactorialCache(n);
    }
    return cache.getDouble(n);
  }

  private static synchronized void expandFactorialCache(int n) {
    if (n >= cache.size()) {
      for (int i = cache.size(); i <= n; i++) {
        fac = fac.multiply(BigInteger.valueOf(i));
        cache.add(BigMath.logBigInteger(fac));
      }
    }
  }
}
