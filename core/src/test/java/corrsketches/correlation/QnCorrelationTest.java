package corrsketches.correlation;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

public class QnCorrelationTest {

  static final double delta = 0.001;

  @Test
  public void shouldComputeQnCorrelation() {
    double[] x;
    double[] y;

    x = new double[] {1, 2, 3};
    y = new double[] {2, 3, 4};
    assertEquals(1., QnCorrelation.correlation(x, y), delta);

    x = new double[] {1, 1, 1};
    y = new double[] {1, 3, 4};
    assertEquals(Double.NaN, QnCorrelation.correlation(x, y), delta);

    x = new double[] {1, 2, 3};
    y = new double[] {1, 1, 1};
    assertEquals(Double.NaN, QnCorrelation.correlation(x, y), delta);

    x = new double[] {-1, -2, -3};
    y = new double[] {1, 2, 3};
    assertEquals(-1., QnCorrelation.correlation(x, y), delta);

    x = new double[] {1, 2, 3};
    y = new double[] {3, 2, 1};
    assertEquals(-1., QnCorrelation.correlation(x, y), delta);

    x = new double[] {1, 2, 3, 4, 5, 4, 3, 2};
    y = new double[] {2, 3, 4, 5, 6, 5, 4, 3};
    assertEquals(1.0, QnCorrelation.correlation(x, y), delta);
  }
}
