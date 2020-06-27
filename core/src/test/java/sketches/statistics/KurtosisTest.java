package sketches.statistics;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

public class KurtosisTest {

  private static final double DELTA = 0.01;

  @Test
  public void testKurtosisPopulationG2() {
    double[] x = new double[]{0, 3, 4, 1, 2, 3, 0, 2, 1, 3, 2, 0, 2, 2, 3, 2, 5, 2, 3, 999};
    assertEquals(15.05143, Kurtosis.g2(x), 0.00001);

    x = new double[]{0, 3, 4, 1, 2, 3, 0, 2, 1, 3, 2, 0};
    assertEquals(-1.193416, Kurtosis.g2(x), 0.000001);

    x = new double[]{-110000, 0, 10000, 0, 10000, 999999};
    assertEquals(1.11692, Kurtosis.g2(x), 0.000001);

    x = new double[]{0, 0, 0, 0, 1};
    assertEquals(0.25, Kurtosis.g2(x), 0.01);

    x = new double[]{-1, 0, 0, 0, 1};
    assertEquals(-0.5, Kurtosis.g2(x), 0.01);

    x = new double[]{0, 0, 1};
    assertEquals(-1.5, Kurtosis.g2(x), 0.01);

    x = new double[]{0, 1};
    assertEquals(-2, Kurtosis.g2(x), 0.01);

    x = new double[]{1};
    assertEquals(Double.NaN, Kurtosis.g2(x), 0.01);
  }

  @Test
  public void testKurtosisSampleG2() {
    double[] x;
    x = new double[]{0, 3, 4, 1, 2, 3, 0, 2, 1, 3, 2, 0, 2, 2, 3, 2, 5, 2, 3, 999};
    assertEquals(19.99843, Kurtosis.G2(x), 0.01);

    x = new double[]{0, 3, 4, 1, 2, 3, 0, 2, 1, 3, 2, 0};
    assertEquals(-1.162872, Kurtosis.G2(x), 0.000001);

    x = new double[]{-110000, 0, 10000, 0, 10000, 999999};
    assertEquals(5.757684, Kurtosis.G2(x), 0.000001);

    x = new double[]{0, 0, 0, 0, 1};
    assertEquals(5., Kurtosis.G2(x), 0.01);

    x = new double[]{-1, 0, 0, 0, 1};
    assertEquals(2, Kurtosis.G2(x), 0.01);

    x = new double[]{0, 0, 1};
    assertEquals(Double.NaN, Kurtosis.G2(x), 0.01);

    x = new double[]{0, 1};
    assertEquals(Double.NaN, Kurtosis.G2(x), 0.01);

    x = new double[]{1};
    assertEquals(Double.NaN, Kurtosis.G2(x), 0.01);
  }

}
