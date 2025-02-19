package corrsketches.statistics;

import static corrsketches.statistics.NearestNeighbors1D.findPoint;
import static corrsketches.statistics.NearestNeighbors1D.kthNearest;
import static corrsketches.statistics.NearestNeighbors1D.kthNearestNonZero;
import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

public class NearestNeighbors1DTest {

  @Test
  public void shouldFindPointsUsingBinarySearch() {
    double[] data = new double[] {1, 2, 3, 4, 5, 6};

    for (int i = 0; i < data.length; i++) {
      assertThat(findPoint(data, data[i])).isEqualTo(i + 1);
    }

    assertThat(findPoint(data, -1)).isEqualTo(0.5);
    assertThat(findPoint(data, -1)).isLessThan(1);

    assertThat(findPoint(data, 3.7)).isEqualTo(3.5);
    assertThat(findPoint(data, 3.7)).isGreaterThan(3);
    assertThat(findPoint(data, 3.7)).isLessThan(4);

    assertThat(findPoint(data, 8)).isEqualTo(6.5);
    assertThat(findPoint(data, 8)).isGreaterThan(6);
  }

  @Test
  public void shouldCountPointsInRange() {
    double[] data = new double[] {1, 2, 3, 4, 5, 6};
    assertThat(NearestNeighbors1D.countPointsInRange(data, 0, 10)).isEqualTo(6);
    assertThat(NearestNeighbors1D.countPointsInRange(data, 0, 3.5)).isEqualTo(3);
    assertThat(NearestNeighbors1D.countPointsInRange(data, 2.5, 3.5)).isEqualTo(1);
  }

  @Test
  public void testFindNearestKNonZero() {
    double[] x = new double[] {1, 2, 4, 8, 16};

    int targetIdx = 0;
    assertThat(kthNearestNonZero(x, targetIdx, 1).kthNearest).isEqualTo(2);
    assertThat(kthNearestNonZero(x, targetIdx, 2).kthNearest).isEqualTo(4);
    assertThat(kthNearestNonZero(x, targetIdx, 3).kthNearest).isEqualTo(8);
    assertThat(kthNearestNonZero(x, targetIdx, 4).kthNearest).isEqualTo(16);
    assertThat(kthNearestNonZero(x, targetIdx, 5).kthNearest).isEqualTo(16);
    assertThat(kthNearestNonZero(x, targetIdx, 6).kthNearest).isEqualTo(16);

    assertThat(kthNearestNonZero(x, targetIdx, 1).distance).isEqualTo(1);
    assertThat(kthNearestNonZero(x, targetIdx, 2).distance).isEqualTo(3);
    assertThat(kthNearestNonZero(x, targetIdx, 3).distance).isEqualTo(7);
    assertThat(kthNearestNonZero(x, targetIdx, 4).distance).isEqualTo(15);
    assertThat(kthNearestNonZero(x, targetIdx, 5).distance).isEqualTo(15);
    assertThat(kthNearestNonZero(x, targetIdx, 6).distance).isEqualTo(15);

    targetIdx = 2;
    assertThat(kthNearestNonZero(x, targetIdx, 1).kthNearest).isEqualTo(2);
    assertThat(kthNearestNonZero(x, targetIdx, 2).kthNearest).isEqualTo(1);
    assertThat(kthNearestNonZero(x, targetIdx, 3).kthNearest).isEqualTo(8);
    assertThat(kthNearestNonZero(x, targetIdx, 4).kthNearest).isEqualTo(16);
    assertThat(kthNearestNonZero(x, targetIdx, 5).kthNearest).isEqualTo(16);
    assertThat(kthNearestNonZero(x, targetIdx, 6).kthNearest).isEqualTo(16);

    assertThat(kthNearestNonZero(x, targetIdx, 1).distance).isEqualTo(2);
    assertThat(kthNearestNonZero(x, targetIdx, 2).distance).isEqualTo(3);
    assertThat(kthNearestNonZero(x, targetIdx, 3).distance).isEqualTo(4);
    assertThat(kthNearestNonZero(x, targetIdx, 4).distance).isEqualTo(12);
    assertThat(kthNearestNonZero(x, targetIdx, 5).distance).isEqualTo(12);
    assertThat(kthNearestNonZero(x, targetIdx, 6).distance).isEqualTo(12);
  }

  @Test
  public void testFindNearestKMixed() {
    double[] x = new double[] {0, 0, 0, 0, 1, 2, 4, 8, 16};

    int targetIdx = 0;
    assertThat(kthNearestNonZero(x, targetIdx, 1).kthNearest).isEqualTo(1);
    assertThat(kthNearestNonZero(x, targetIdx, 2).kthNearest).isEqualTo(1);
    assertThat(kthNearestNonZero(x, targetIdx, 3).kthNearest).isEqualTo(1);
    assertThat(kthNearestNonZero(x, targetIdx, 4).kthNearest).isEqualTo(1);
    assertThat(kthNearestNonZero(x, targetIdx, 5).kthNearest).isEqualTo(2);
    assertThat(kthNearestNonZero(x, targetIdx, 6).kthNearest).isEqualTo(4);

    assertThat(kthNearestNonZero(x, targetIdx, 1).kthNearest).isEqualTo(1);
    assertThat(kthNearestNonZero(x, targetIdx, 2).kthNearest).isEqualTo(1);
    assertThat(kthNearestNonZero(x, targetIdx, 3).kthNearest).isEqualTo(1);
    assertThat(kthNearestNonZero(x, targetIdx, 4).kthNearest).isEqualTo(1);
    assertThat(kthNearestNonZero(x, targetIdx, 5).kthNearest).isEqualTo(2);
    assertThat(kthNearestNonZero(x, targetIdx, 6).kthNearest).isEqualTo(4);

    assertThat(kthNearestNonZero(x, targetIdx, 1).distance).isEqualTo(1);
    assertThat(kthNearestNonZero(x, targetIdx, 2).distance).isEqualTo(1);
    assertThat(kthNearestNonZero(x, targetIdx, 3).distance).isEqualTo(1);
    assertThat(kthNearestNonZero(x, targetIdx, 4).distance).isEqualTo(1);
    assertThat(kthNearestNonZero(x, targetIdx, 5).distance).isEqualTo(2);
    assertThat(kthNearestNonZero(x, targetIdx, 6).distance).isEqualTo(4);

    assertThat(kthNearestNonZero(x, targetIdx, 1).k).isEqualTo(4);
    assertThat(kthNearestNonZero(x, targetIdx, 2).k).isEqualTo(4);
    assertThat(kthNearestNonZero(x, targetIdx, 3).k).isEqualTo(4);
    assertThat(kthNearestNonZero(x, targetIdx, 4).k).isEqualTo(4);
    assertThat(kthNearestNonZero(x, targetIdx, 5).k).isEqualTo(5);
    assertThat(kthNearestNonZero(x, targetIdx, 6).k).isEqualTo(6);
  }

  @Test
  public void testFindNearestK() {
    double[] x = new double[] {1, 2, 4, 8, 16};

    int targetIdx = 0;
    assertThat(kthNearest(x, targetIdx, 1).kthNearest).isEqualTo(2);
    assertThat(kthNearest(x, targetIdx, 2).kthNearest).isEqualTo(4);
    assertThat(kthNearest(x, targetIdx, 3).kthNearest).isEqualTo(8);
    assertThat(kthNearest(x, targetIdx, 4).kthNearest).isEqualTo(16);
    assertThat(kthNearest(x, targetIdx, 5).kthNearest).isEqualTo(16);
    assertThat(kthNearest(x, targetIdx, 6).kthNearest).isEqualTo(16);

    assertThat(kthNearest(x, targetIdx, 1).distance).isEqualTo(1);
    assertThat(kthNearest(x, targetIdx, 2).distance).isEqualTo(3);
    assertThat(kthNearest(x, targetIdx, 3).distance).isEqualTo(7);
    assertThat(kthNearest(x, targetIdx, 4).distance).isEqualTo(15);
    assertThat(kthNearest(x, targetIdx, 5).distance).isEqualTo(15);
    assertThat(kthNearest(x, targetIdx, 6).distance).isEqualTo(15);

    targetIdx = 2;
    assertThat(kthNearest(x, targetIdx, 1).kthNearest).isEqualTo(2);
    assertThat(kthNearest(x, targetIdx, 2).kthNearest).isEqualTo(1);
    assertThat(kthNearest(x, targetIdx, 3).kthNearest).isEqualTo(8);
    assertThat(kthNearest(x, targetIdx, 4).kthNearest).isEqualTo(16);
    assertThat(kthNearest(x, targetIdx, 5).kthNearest).isEqualTo(16);
    assertThat(kthNearest(x, targetIdx, 6).kthNearest).isEqualTo(16);

    assertThat(kthNearest(x, targetIdx, 1).distance).isEqualTo(2);
    assertThat(kthNearest(x, targetIdx, 2).distance).isEqualTo(3);
    assertThat(kthNearest(x, targetIdx, 3).distance).isEqualTo(4);
    assertThat(kthNearest(x, targetIdx, 4).distance).isEqualTo(12);
    assertThat(kthNearest(x, targetIdx, 5).distance).isEqualTo(12);
    assertThat(kthNearest(x, targetIdx, 6).distance).isEqualTo(12);
  }
}
