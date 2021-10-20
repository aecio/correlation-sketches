package corrsketches.correlation;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import corrsketches.correlation.PearsonCorrelation.ConfidenceInterval;
import org.junit.jupiter.api.Test;

public class PearsonCorrelationTest {

  static final double delta = 0.001;

  @Test
  public void shouldComputeCorrelationCoefficient() {
    double[] x = {1, 2, 3};
    double[] y = {1, 2, 3};
    assertEquals(1., PearsonCorrelation.coefficient(x, y), delta);
  }

  @Test
  public void shouldComputeCorrelationCoefficient2() {
    double[] x = {0, 0, 0};
    double[] y = {2, 2, 2};
    assertEquals(Double.NaN, PearsonCorrelation.coefficient(x, y), delta);
  }

  @Test
  public void shouldComputeCorrelationCoefficient3() {
    double[] x;
    double[] y;

    x = new double[] {0, 0, 0.0001};
    y = new double[] {2, 2, 2};
    assertEquals(Double.NaN, PearsonCorrelation.coefficient(x, y), delta);

    x = new double[] {39.2, 40.21, 15.41, 13.64, 33.2, 44.65, 30.91, 1.91};
    y = new double[] {0.31, 0.31, 0.31, 0.31, 0.31, 0.31, 0.31, 0.31};
    assertEquals(Double.NaN, PearsonCorrelation.coefficient(x, y), delta);

    x = new double[] {29815.0, 76201.0, 3254.0, 79000.0, 15974.0, 3288.0, 27777.0};
    y = new double[] {145.8, 145.8, 145.8, 145.8, 145.8, 145.8, 145.8};
    assertEquals(Double.NaN, PearsonCorrelation.coefficient(x, y), delta);
  }

  @Test
  public void shouldComputeCorrelationCoefficient5() {
    double[] x;
    double[] y;

    x = new double[] {1, 2, 3, 4};
    y = new double[] {2, 4, 1, 5};
    assertEquals(0.424264, PearsonCorrelation.coefficient(x, y), delta);

    x = new double[] {2, 4, 1, 5, 3, 7};
    y = new double[] {1, 2, 3, 4, 0, 1};
    assertEquals(-0.020965, PearsonCorrelation.coefficient(x, y), delta);

    x = new double[] {2, 4, 6, 7, 2, 2};
    y = new double[] {1, 2, 3, 4, 0, 1};
    assertEquals(0.96532553, PearsonCorrelation.coefficient(x, y), delta);
  }

  @Test
  public void shouldComputeCorrelationCoefficient5PVC() {
    double[] x;
    double[] y;

    x = new double[] {1, 2, 3, 4};
    y = new double[] {2, 4, 1, 5};
    assertEquals(0.424264, PCVCorrelation.correlation(x, y), delta);

    x = new double[] {2, 4, 1, 5, 3, 7};
    y = new double[] {1, 2, 3, 4, 0, 1};
    assertEquals(-0.020965, PCVCorrelation.correlation(x, y), delta);

    x = new double[] {2, 4, 6, 7, 2, 2};
    y = new double[] {1, 2, 3, 4, 0, 1};
    assertEquals(0.96532553, PCVCorrelation.correlation(x, y), delta);
  }

  @Test
  public void shouldComputeCorrelationCoefficient4() {
    //    double[] x = {0,0,0,0,1,1,0,0,0,0,0,0};
    double[] y = {0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0};
    double[] x = {1, 1, 1, 1, 1, 2, 1, 1, 1, 1, 1, 1};
    assertEquals(1., PearsonCorrelation.coefficient(x, y), delta);
  }

  @Test
  public void shouldComputeOneTailedTTestPValue() {
    assertEquals(0.24302101, PearsonCorrelation.pValueOneTailed(0.25, 10), delta);
    assertEquals(0.07055664, PearsonCorrelation.pValueOneTailed(0.50, 10), delta);
    assertEquals(0.14388153, PearsonCorrelation.pValueOneTailed(0.25, 20), delta);
    assertEquals(0.01238478, PearsonCorrelation.pValueOneTailed(0.50, 20), delta);
  }

  @Test
  public void shouldComputeTwoTailedTTestPValue() {
    assertEquals(0.48604202, PearsonCorrelation.pValueTwoTailed(0.25, 10), delta);
    assertEquals(0.14111328, PearsonCorrelation.pValueTwoTailed(0.50, 10), delta);
    assertEquals(0.28776307, PearsonCorrelation.pValueTwoTailed(0.25, 20), delta);
    assertEquals(0.02476956, PearsonCorrelation.pValueTwoTailed(0.50, 20), delta);
  }

  @Test
  public void shouldComputeIfCorrelationCoefficientIsSignificant() {
    assertFalse(PearsonCorrelation.isSignificant(.50, 10, 0.05));
    assertTrue(PearsonCorrelation.isSignificant(.50, 100, 0.05));
  }

  @Test
  public void shouldComputeConfidenceIntervals1() {
    ConfidenceInterval interval = PearsonCorrelation.confidenceInterval(0.5, 10, .95);
    assertEquals(-0.189, interval.lowerBound, delta);
    assertEquals(0.859, interval.upperBound, delta);
  }

  @Test
  public void shouldComputeConfidenceIntervals2() {
    ConfidenceInterval interval = PearsonCorrelation.confidenceInterval(0.5, 10, .80);
    assertEquals(0.065, interval.lowerBound, delta);
    assertEquals(0.775, interval.upperBound, delta);
  }

  @Test
  public void shouldComputeConfidenceIntervals3() {
    ConfidenceInterval interval = PearsonCorrelation.confidenceInterval(-0.654, 34, .95);
    assertEquals(-0.812, interval.lowerBound, delta);
    assertEquals(-0.406, interval.upperBound, delta);
  }
}
