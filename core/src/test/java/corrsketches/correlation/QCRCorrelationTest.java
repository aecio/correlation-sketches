package corrsketches.correlation;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

public class QCRCorrelationTest {

  static final double delta = 0.01;

  @Test
  public void shouldComputeCorrelationCoefficients() {
    double[] x;
    double[] y;

    x = new double[] {1, 2, 3, 4, 5, 4, 3, 2};
    y = new double[] {2, 3, 4, 5, 6, 5, 4, 3};
    assertEquals(0.750, QCRCorrelation.coefficient(x, y), delta);

    x = new double[] {1, 2, 4, 5, 4, 3, 2};
    y = new double[] {2, 3, 4, 5, 5, 4, 3};
    assertEquals(0.857, QCRCorrelation.coefficient(x, y), delta);

    x = new double[] {1, 2, 4, 5, 4, 3, 2};
    y = new double[] {2, 3, 4, 5, 5, 4, 3};
    assertEquals(0.857, QCRCorrelation.coefficient(x, y), delta);

    x =
        new double[] {
          185, 185, 185, 180, 190, 175, 190, 180, 175, 165, 165, 175, 175, 165, 160,
          160, 175, 160, 175, 175, 175, 160, 160, 160, 160, 160, 160, 175, 160, 160,
          160, 185, 160, 160, 160L
        };
    y =
        new double[] {
          882, 888, 892, 895, 899, 902, 905, 905, 905, 910, 910, 914, 915, 915, 918,
          920, 922, 923, 924, 926, 929, 929, 929, 929, 929, 930, 931, 932, 932, 938,
          943, 948, 948, 957, 972L
        };
    assertEquals(-0.371, QCRCorrelation.coefficient(x, y), delta);
  }
}
