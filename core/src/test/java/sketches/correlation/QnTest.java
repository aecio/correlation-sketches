package sketches.correlation;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Random;
import org.junit.jupiter.api.Test;

public class QnTest {

  static final double delta = 0.001;

  @Test
  public void shouldGetOrderStatistic() {
    double[] x;

    x = new double[] {1, 2, 3};
    assertEquals(1, Qn.findKthOrderStatistic(x, 2, 1), delta);
    assertEquals(2, Qn.findKthOrderStatistic(x, 2, 2), delta);
    try {
      Qn.findKthOrderStatistic(x, 2, 3); // outside valid range
    } catch (IllegalArgumentException e) {
      // great, should fail when outside valid range
    }

    assertEquals(1, Qn.findKthOrderStatistic(x, 3, 1), delta);
    assertEquals(2, Qn.findKthOrderStatistic(x, 3, 2), delta);
    assertEquals(3, Qn.findKthOrderStatistic(x, 3, 3), delta);

    x = new double[] {3, 2, 1};
    assertEquals(1, Qn.findKthOrderStatistic(x, 3, 1), delta);
    assertEquals(2, Qn.findKthOrderStatistic(x, 3, 2), delta);
    assertEquals(3, Qn.findKthOrderStatistic(x, 3, 3), delta);

    assertEquals(2, Qn.findKthOrderStatistic(x, 2, 1), delta);
    assertEquals(3, Qn.findKthOrderStatistic(x, 2, 2), delta);

    assertEquals(3, Qn.findKthOrderStatistic(x, 1, 1), delta);

    x = new double[] {25, 20, 25};
    assertEquals(25, Qn.findKthOrderStatistic(x, 1, 1), delta);
    assertEquals(20, Qn.findKthOrderStatistic(x, 2, 1), delta);
    assertEquals(20, Qn.findKthOrderStatistic(x, 3, 1), delta);

    assertEquals(25, Qn.findKthOrderStatistic(x, 2, 2), delta);
    assertEquals(25, Qn.findKthOrderStatistic(x, 3, 3), delta);
  }

  @Test
  public void shouldComputeQnStatistic() {
    // Expected corrected values computed using the 'robustbase' R package.

    double[] x;

    x = new double[] {1, 2, 3};
    assertEquals(2.205048, Qn.estimateScale(x), delta);

    x = new double[] {0, 0, 0};
    assertEquals(0, Qn.estimateScale(x), delta);

    x = new double[] {5, 10, 25, 35};
    assertEquals(17.08327, Qn.estimateScale(x), delta);

    x = new double[] {5000, 1, 123, 45476, 3, 3435};
    assertEquals(4662.569, Qn.estimateScale(x), delta);

    x = new double[] {0, 0, 0, 1234, 1239, 1345};
    assertEquals(150.7999, Qn.estimateScale(x), delta);

    x = new double[] {0, 0, 0, 0, 1234, 1239, 1345};
    assertEquals(0, Qn.estimateScale(x), delta);

    x = new double[] {123, 443423, -121673, 4432, 0, 3987};
    assertEquals(6021.127, Qn.estimateScale(x), delta);

    x = new double[] {123, 33, 655, 8758, 0, 3987};
    assertEquals(889.8552, Qn.estimateScale(x), delta);

    x = new double[] {46, 3, 7583, 475, 78687, 347, 575};
    assertEquals(655.5714, Qn.estimateScale(x), delta);

    x =
        new double[] {
          46, 3, 7583, 475, 78687, 347, 575, 123, 33, 655, 8758, 0, 3987, 0, 0, 0, 1234, 1239,
          12345, 3, 4, 6, 9, 1235, 39, 48
        };
    assertEquals(232.674, Qn.estimateScale(x), delta);

    x =
        new double[] {
          12346, 46, 3, 7583, 475, 78687, 347, 575, 123, 33, 655, 8758, 0, 3987, 0, 0, 0, 1234,
          1239, 12345, 3, 4, 6, 9, 1235, 39, 48
        };
    assertEquals(250.0389, Qn.estimateScale(x), delta);

    x = new double[] {3, 7583, 475, 78687, 347, 575, 123, 33, 655, 8758, 0, 3987};
    assertEquals(798.4005, Qn.estimateScale(x), delta);

    x = new double[] {-1, 7583, 475, 0.001, 0.347, 575, 123, 33};
    assertEquals(182.8587, Qn.estimateScale(x), delta);

    x = new double[] {1, 2, 3, 4, 5};
    assertEquals(1.872976, Qn.estimateScale(x), delta);

    x = new double[] {2, 3, 4, 4, 4};
    assertEquals(0, Qn.estimateScale(x), delta);

    x =
        new double[] {
          -1.1339252345842308,
          -1.4772810541263675,
          -1.8206368736685037,
          -1.3734232781685456,
          -0.9262096826685879
        };
    assertEquals(0.4485742, Qn.estimateScale(x), delta);

    x = new double[] {-0.7550621275995539, -0.37753106379977697, -0.37753106379977686, 0.0, 0.0};
    assertEquals(0.7071067, Qn.estimateScale(x), delta);
  }

  @Test
  public void shouldComputeQnCorrelation() {
    double[] x;
    double[] y;

    x = new double[] {1, 2, 3};
    y = new double[] {2, 3, 4};
    assertEquals(1., Qn.correlation(x, y), delta);

    x = new double[] {1, 1, 1};
    y = new double[] {1, 3, 4};
    assertEquals(Double.NaN, Qn.correlation(x, y), delta);

    x = new double[] {1, 2, 3};
    y = new double[] {1, 1, 1};
    assertEquals(Double.NaN, Qn.correlation(x, y), delta);

    x = new double[] {-1, -2, -3};
    y = new double[] {1, 2, 3};
    assertEquals(-1., Qn.correlation(x, y), delta);

    x = new double[] {1, 2, 3};
    y = new double[] {3, 2, 1};
    assertEquals(-1., Qn.correlation(x, y), delta);

    x = new double[] {1, 2, 3, 4, 5, 4, 3, 2};
    y = new double[] {2, 3, 4, 5, 6, 5, 4, 3};
    assertEquals(1.0, Qn.correlation(x, y), delta);
  }

  @Test
  public void shouldComputeQnWithoutOverflowing() {
    Random random = new Random(0);
    double[] x = new double[1000000];
    for (int i = 0; i < x.length; i++) {
      x[i] = random.nextDouble();
    }
    // should not throw an exception
    Qn.estimateScale(x);
  }

  @Test
  public void shouldComputeWeightedHighMedianWithoutOverflowing() {
    Random random = new Random(0);
    final int n = 100000;
    double[] x = new double[n];
    int[] iw = new int[n];
    for (int i = 0; i < x.length; i++) {
      x[i] = random.nextDouble();
      iw[i] = random.nextInt(Integer.MAX_VALUE);
    }
    // should not throw an exception
    Qn.weightedHighMedian(x, iw, x.length);
  }

  @Test
  public void shouldFindTheWeightedHighMedian() {
    double[] x;
    int[] iw;

    x = new double[] {1, 2, 3, 4};
    iw = new int[] {1000000000, 1000000000, 1000000000, 1000000000};
    assertEquals(3, Qn.weightedHighMedian(x, iw, x.length));

    x = new double[] {1, 2, 3, 4};
    iw = new int[] {1000000000, 1000000000, 1000000000, 1000000000};
    assertEquals(2, Qn.weightedHighMedian(x, iw, 3));

    x = new double[] {1, 2, 3, 4};
    iw = new int[] {2, 2, 2, 2};
    assertEquals(3, Qn.weightedHighMedian(x, iw, x.length));
  }
}
