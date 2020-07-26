package sketches.statistics;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Arrays;
import org.junit.jupiter.api.Test;
import sketches.correlation.PearsonCorrelation;
import sketches.statistics.Stats.Extent;

public class StatsTests {

  @Test
  public void testArrayExtent() {
    double[] x;
    Extent extent;

    x = new double[]{1, 2, 3, 4};
    extent = Stats.extent(x);
    assertEquals(1.0, extent.min);
    assertEquals(4.0, extent.max);

    x = new double[]{-10, 0, 10};
    extent = Stats.extent(x);
    assertEquals(-10.0, extent.min);
    assertEquals(10.0, extent.max);

    x = new double[]{0, 0, 0};
    extent = Stats.extent(x);
    assertEquals(0.0, extent.min);
    assertEquals(0.0, extent.max);

    x = new double[]{-1.0/Double.MIN_VALUE, 0, 1/Double.MIN_VALUE};
    extent = Stats.extent(x);
    assertEquals(Double.NEGATIVE_INFINITY, extent.min);
    assertEquals(Double.POSITIVE_INFINITY, extent.max);
  }

  @Test
  public void testArrayDotProduct() {
    double[] x;
    double dotProduct;

    x = new double[]{1, 2};
    dotProduct = Stats.dot(x, x);
    assertEquals((1*1+2*2)/2.0, dotProduct);
  }

  @Test
  public void testArrayUnitRange() {
    double[] x, y;
    double[] xUnitRange, yUnitRange;

    x = new double[]{1, 2, 3};
    xUnitRange = Stats.unitRange(x, 1, 3);

    y = new double[]{1.5, 2, 2.2};
    yUnitRange = Stats.unitRange(y, 1.5, 2.2);

    System.out.println(Arrays.toString(yUnitRange));

    assertEquals(0.0, xUnitRange[0]);
    assertEquals(0.5, xUnitRange[1]);
    assertEquals(1.0, xUnitRange[2]);

    assertEquals(0.0, yUnitRange[0]);
    assertEquals(1.0, yUnitRange[2]);

    assertEquals(PearsonCorrelation.coefficient(x, y), PearsonCorrelation.coefficient(xUnitRange, yUnitRange), 1e-10);
  }

}
