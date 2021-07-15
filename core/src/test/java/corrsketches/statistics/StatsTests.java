package corrsketches.statistics;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

import corrsketches.correlation.PearsonCorrelation;
import corrsketches.statistics.Stats.Extent;
import java.util.Arrays;
import org.junit.jupiter.api.Test;

public class StatsTests {

  @Test
  public void testArrayExtent() {
    double[] x;
    Extent extent;

    x = new double[] {1, 2, 3, 4};
    extent = Stats.extent(x);
    assertEquals(1.0, extent.min);
    assertEquals(4.0, extent.max);

    x = new double[] {-10, 0, 10};
    extent = Stats.extent(x);
    assertEquals(-10.0, extent.min);
    assertEquals(10.0, extent.max);

    x = new double[] {0, 0, 0};
    extent = Stats.extent(x);
    assertEquals(0.0, extent.min);
    assertEquals(0.0, extent.max);

    x = new double[] {-1.0 / Double.MIN_VALUE, 0, 1 / Double.MIN_VALUE};
    extent = Stats.extent(x);
    assertEquals(Double.NEGATIVE_INFINITY, extent.min);
    assertEquals(Double.POSITIVE_INFINITY, extent.max);
  }

  @Test
  public void testArrayDotProductDividedByLength() {
    double[] x = new double[] {1, 2};
    double dotn = Stats.dotn(x, x);
    assertEquals((1 * 1 + 2 * 2) / 2.0, dotn);
  }

  @Test
  public void testMedian() {
    double[] x = new double[] {1, 4, 3, 2, 5};
    double[] original = Arrays.copyOf(x, x.length);

    assertEquals(3, Stats.median(x));
    assertEquals(3, Stats.median(x, 5));
    assertEquals((2 + 3) / 2d, Stats.median(x, 4));
    assertEquals(3, Stats.median(x, 3));
    assertThat(x).isEqualTo(original);

    assertEquals(3, Stats.median(x, 3, true));
    assertThat(x).isNotEqualTo(original); // should have changed order of elements
  }

  @Test
  public void testArrayUnitRange() {

    double[] x = new double[] {1, 2, 3};
    double[] xUnitRange = Stats.unitRange(x, 1, 3);

    double[] y = new double[] {1.5, 2, 2.2};
    double[] yUnitRange = Stats.unitRange(y, 1.5, 2.2);

    System.out.println(Arrays.toString(yUnitRange));

    assertEquals(0.0, xUnitRange[0]);
    assertEquals(0.5, xUnitRange[1]);
    assertEquals(1.0, xUnitRange[2]);

    assertEquals(0.0, yUnitRange[0]);
    assertEquals(1.0, yUnitRange[2]);

    assertEquals(
        PearsonCorrelation.coefficient(x, y),
        PearsonCorrelation.coefficient(xUnitRange, yUnitRange),
        1e-10);
  }
}
