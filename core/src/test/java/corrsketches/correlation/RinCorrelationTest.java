package corrsketches.correlation;

import static org.junit.jupiter.api.Assertions.assertEquals;

import corrsketches.correlation.PearsonCorrelation.ConfidenceInterval;
import org.junit.jupiter.api.Test;

public class RinCorrelationTest {

  @Test
  public void shouldComputeRinCorrelationCoefficient() {
    double[] x;
    double[] y;
    ConfidenceInterval ci;
    double rrin;

    x = new double[] {1.3, 2.4, 30.5};
    y = new double[] {1.0, 2.0, 2.0};
    rrin = RinCorrelation.coefficient(x, y);
    ci = PearsonCorrelation.confidenceInterval(rrin, x.length, .95);

    assertEquals(0.8660254, rrin, 0.00001);
    assertEquals(Double.NaN, ci.lowerBound);
    assertEquals(Double.NaN, ci.upperBound);

    y = new double[] {1.0, 2.0, 2.0, 3.9};
    x = new double[] {1.3, 9.0, 2.4, 3.5};
    rrin = RinCorrelation.coefficient(x, y);
    ci = PearsonCorrelation.confidenceInterval(rrin, x.length, .95);

    assertEquals(0.6153274, rrin, 0.00001);
    assertEquals(-0.8461710, ci.lowerBound, 0.00001);
    assertEquals(0.9905939, ci.upperBound, 0.00001);

    y = new double[] {1.0, 2.0, 2.0, 4.0};
    x = new double[] {1.0, 2.0, 2.0, 4.0};
    rrin = RinCorrelation.coefficient(x, y);
    ci = PearsonCorrelation.confidenceInterval(rrin, x.length, .95);

    assertEquals(1, rrin, 0.00001);
    assertEquals(1, ci.lowerBound, 0.00001);
    assertEquals(1, ci.upperBound, 0.00001);

    y = new double[] {3.0, 2.0, 5.0, 1.0, 4.0};
    x = new double[] {3.0, 4.0, 1.0, 5.0, 2.0};
    rrin = RinCorrelation.coefficient(x, y);
    ci = PearsonCorrelation.confidenceInterval(rrin, x.length, .95);

    assertEquals(-1, rrin, 0.00001);
    assertEquals(-1, ci.lowerBound, 0.00001);
    assertEquals(-1, ci.upperBound, 0.00001);
  }
}
