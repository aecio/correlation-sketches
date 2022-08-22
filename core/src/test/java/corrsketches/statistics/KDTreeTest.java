package corrsketches.statistics;

import static org.assertj.core.api.Assertions.assertThat;

import corrsketches.util.KDTree;
import corrsketches.util.KDTree.Neighbor;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

public class KDTreeTest {

  @ParameterizedTest
  @EnumSource(value = KDTree.Distance.class)
  public void shouldFindNearestPointsUsingKDTree(KDTree.Distance distance) {

    double[] data = new double[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};

    // generate the data vectors that will be indexed and queried
    final int d = 3;
    double[][] dataNd = new double[data.length][d];
    for (int i = 0; i < data.length; i++) {
      for (int j = 0; j < d; j++) {
        dataNd[i][j] = data[i];
      }
    }

    // build the tree that will be tested
    KDTree tree = new KDTree(dataNd, distance);

    for (int i = 0; i < data.length; i++) {
      // nearest() query should be the query itself
      final double[] q = dataNd[i];
      Neighbor nn = tree.nearest(q);
      assertThat(nn.index).isEqualTo(i);
      assertThat(nn.distance).isEqualTo(0);
      assertThat(nn.key).containsExactly(q);

      // knn() query should return k, as long as k is smaller than the data size
      for (int k = 1; k < 2 * d; k++) {
        final Neighbor[] knn = tree.knn(dataNd[i], k);
        final int expected = Math.min(k, data.length);
        assertThat(knn.length).isEqualTo(expected);
      }
    }

    // test radius up to 3, queries start at 3 after and 3 before to avoid points close to the
    // vector boundaries that different expected sizes
    for (int i = 3; i < data.length - 3; i++) {
      for (double radius = 1.0; radius <= 3; radius++) {
        final double[] q = dataNd[i];
        // r = 0.9999.., 1.99999..., etc. thus, the query should not return points
        // with distance exactly equal the radius.
        double r = Math.nextDown(radius);

        // should find the query point (1) and one p more points at each side
        // so expected count is 1+2*p where p = floor(r)
        assertThat(tree.countInRange(q, r)).isEqualTo((int) (1 + 2 * Math.floor(r)));
      }
    }
  }
}
