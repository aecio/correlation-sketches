package corrsketches.benchmark.utils;

import java.math.BigInteger;

/**
 * Provides some mathematical operations on {@code BigInteger}. Based on code from <a
 * href="https://stackoverflow.com/questions/6827516/logarithm-for-biginteger">
 * https://stackoverflow.com/questions/6827516/logarithm-for-biginteger</a>.
 */
public class BigMath {

  public static final double LOG_2 = Math.log(2.0);
  public static final double LOG_10 = Math.log(10.0);

  // numbers greater than 10^MAX_DIGITS_10 or e^MAX_DIGITS_E are
  // considered unsafe ('too big') for floating point operations
  private static final int MAX_DIGITS_10 = 294;
  private static final int MAX_DIGITS_2 = 977; // ~ MAX_DIGITS_10 * LN(10)/LN(2)
  private static final int MAX_DIGITS_E = 677; // ~ MAX_DIGITS_10 * LN(10)

  /**
   * Computed the natural logarithm of the input. Uses the identity: log(a) = log(a/2^k) + k log(2)
   *
   * @param value
   * @return ln(value)
   */
  public static double logBigInteger(BigInteger value) {
    if (value.signum() < 1) return value.signum() < 0 ? Double.NaN : Double.NEGATIVE_INFINITY;
    int blex = value.bitLength() - MAX_DIGITS_2; // any value in 60..1023 works here
    if (blex > 0) {
      value = value.shiftRight(blex);
    }
    double res = Math.log(value.doubleValue());
    return blex > 0 ? res + blex * LOG_2 : res;
  }
}
