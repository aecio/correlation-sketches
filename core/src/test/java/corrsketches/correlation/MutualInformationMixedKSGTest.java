package corrsketches.correlation;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.byLessThan;

import org.junit.jupiter.api.Test;

public class MutualInformationMixedKSGTest {

  final double DELTA = 0.0000000001;

  @Test
  public void testDiscreteContinuousMI() {
    int k = 3;
    double[] x = new double[] {1, 1, 2, 2, 3, 3};

    //
    // Case 1
    //
    double[] y1 = new double[] {1.0, 1.0, 2.3, 2.4, 3.1, 3.5};
    assertThat(MutualInformationMixedKSG.mi(x, y1, k))
        .isCloseTo(0.468975134129588, byLessThan(DELTA));

    double[] y2 = new double[] {1.0, 1.0, 2.3, 2.4, 3.1, 3.2};
    assertThat(MutualInformationMixedKSG.mi(x, y2, k))
        .isCloseTo(0.8689751341295882, byLessThan(DELTA));

    double[] y3 = new double[] {1.0, 1.0, 2.3, 3.4, 3.1, 0.5};
    assertThat(MutualInformationMixedKSG.mi(x, y3, k))
        .isCloseTo(0.14397513412958812, byLessThan(DELTA));

    double[] y4 = new double[] {1.0, 1.0, 2.3, 3.4, 3.1, 3.2};
    assertThat(MutualInformationMixedKSG.mi(x, y4, k))
        .isCloseTo(0.4023084674629213, byLessThan(DELTA));

    double[] y5 = new double[] {3.0, 1.0, 2.3, 3.4, 3.1, 3.2};
    assertThat(MutualInformationMixedKSG.mi(x, y5, k))
        .isCloseTo(0.6134195785740324, byLessThan(DELTA));

    //
    // Case 2, number of repeated discrete variables larger than k
    //
    double[] y;
    x = new double[] {1, 1, 1, 1, 2, 3};

    y = new double[] {1.0, 1.0, 1.0, 1.0, 3.15, 3.2};
    assertThat(MutualInformationMixedKSG.mi(x, y, k))
        .isCloseTo(0.5523084674629213, byLessThan(DELTA));

    y = new double[] {3.0, 100.0, 2.3, 3.4, 3.15, 3.2};
    assertThat(MutualInformationMixedKSG.mi(x, y, k))
        .isCloseTo(0.4189751341295881, byLessThan(DELTA));

    y = new double[] {3.0, 100.0, 2.3, 3.4, 3.1, 103.2};
    assertThat(MutualInformationMixedKSG.mi(x, y, k))
        .isCloseTo(0.49675291190736587, byLessThan(DELTA));

    y = new double[] {3.0, 100.0, 2.3, 3.4, 100000, 103.2};
    assertThat(MutualInformationMixedKSG.mi(x, y, k))
        .isCloseTo(0.08564180079625461, byLessThan(DELTA));

    y = new double[] {1e-18, 1e-18, 0.0001, 3.4, 1e-19, 3.45};
    assertThat(MutualInformationMixedKSG.mi(x, y, k))
        .isCloseTo(0.46897513412958813, byLessThan(DELTA));

    y = new double[] {0.57360673, 1.42928002, 0.20841156, -0.97415809, 0.55514294, -0.47505342};
    assertThat(MutualInformationMixedKSG.mi(x, y, k))
        .isCloseTo(0.5023084674629215, byLessThan(DELTA));

    //
    // Case 3, {+1, -1} distribution in one side
    //
    x = new double[] {1, 1, 1, 1, 1, -1, -1, -1, -1, -1};

    y = new double[] {1.0, 1.0, 1.0, 1.0, 3.15, 3.2, 1.0, 3.0, 900, 0.0};
    assertThat(MutualInformationMixedKSG.mi(x, y, k))
        .isCloseTo(0.3161102817051028, byLessThan(DELTA));

    y = new double[] {0.0, 0.0, 0.0, 0.0, 0.0, .0, 0.0, 0.0, 0.0, 0.0};
    assertThat(MutualInformationMixedKSG.mi(x, y, k))
        .isCloseTo(0.05083250392732497, byLessThan(DELTA));
  }
}
