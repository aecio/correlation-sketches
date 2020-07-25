package sketches.statistics;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;
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

}
