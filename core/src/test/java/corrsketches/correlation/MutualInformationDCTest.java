package corrsketches.correlation;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.byLessThan;

import org.junit.jupiter.api.Test;

public class MutualInformationDCTest {

  final double DELTA = 0.0001;

  @Test
  public void testDiscreteContinuousMI() {
    int k = 3;
    double base = Math.exp(1);
    int[] d1 = new int[] {1, 1, 2, 2, 3, 3};

    //
    // Case 1
    //
    double[] c1 = new double[] {1.0, 1.0, 2.3, 2.4, 3.1, 0.5};
    assertThat(MutualInformationDC.mi(d1, c1, k, base)).isCloseTo(0.5889, byLessThan(DELTA));

    double[] c2 = new double[] {1.0, 1.0, 2.3, 2.4, 3.1, 3.2};
    assertThat(MutualInformationDC.mi(d1, c2, k, base)).isCloseTo(1.2833, byLessThan(DELTA));

    double[] c3 = new double[] {1.0, 1.0, 2.3, 3.4, 3.1, 0.5};
    assertThat(MutualInformationDC.mi(d1, c3, k, base)).isCloseTo(0.2972, byLessThan(DELTA));

    double[] c4 = new double[] {1.0, 1.0, 2.3, 3.4, 3.1, 3.2};
    assertThat(MutualInformationDC.mi(d1, c4, k, base)).isCloseTo(0.7833, byLessThan(DELTA));

    double[] c5 = new double[] {3.0, 1.0, 2.3, 3.4, 3.1, 3.2};
    assertThat(MutualInformationDC.mi(d1, c5, k, base)).isCloseTo(-0.0083, byLessThan(DELTA));

    //
    // Case 2, different discrete variable
    //
    int[] d2 = new int[] {1, 2, 2, 2, 3, 3};

    double[] c6 = new double[] {3.0, 100.0, 2.3, 3.4, 3.1, 3.2};
    assertThat(MutualInformationDC.mi(d2, c6, k, base)).isCloseTo(0.1111, byLessThan(DELTA));

    double[] c7 = new double[] {3.0, 100.0, 2.3, 3.4, 3.1, 103.2};
    assertThat(MutualInformationDC.mi(d2, c7, k, base)).isCloseTo(-0.2361, byLessThan(DELTA));

    double[] c8 = new double[] {3.0, 100.0, 2.3, 3.4, Double.NaN, 103.2};
    assertThat(MutualInformationDC.mi(d2, c8, k, base)).isCloseTo(0.5139, byLessThan(DELTA));

    //
    // Case 3, with NaNs, +Inf, and -Inf.
    //
    int[] d3 = new int[] {0, 1, 2, 2, 2, 3, 4, 5};

    double[] c9 = new double[] {3.0, 100.0, -2.3, -103.4, 0, 0, 0, 0};
    assertThat(MutualInformationDC.mi(d3, c9, k, base)).isCloseTo(-0.1987, byLessThan(DELTA));

    double[] c10 = new double[] {0, 0, 0, 1e99, 0, 0, 0, 0};
    assertThat(MutualInformationDC.mi(d3, c10, k, base)).isCloseTo(-0.4008, byLessThan(DELTA));

    double[] c11 = new double[] {0, 0, 0, 1e99, 0, Double.POSITIVE_INFINITY, 0, 0};
    assertThat(MutualInformationDC.mi(d3, c11, k, base)).isCloseTo(-0.3383, byLessThan(DELTA));

    double[] c12 = new double[] {0, 0, 0, 1e99, 0, Double.POSITIVE_INFINITY, Double.NaN, 0};
    assertThat(MutualInformationDC.mi(d3, c12, k, base)).isCloseTo(-0.2633, byLessThan(DELTA));

    double[] c13 = new double[] {0, -1, 0, 1e99, 0, Double.POSITIVE_INFINITY, Double.NaN, 0};
    assertThat(MutualInformationDC.mi(d3, c13, k, base)).isCloseTo(-0.2321, byLessThan(DELTA));

    double[] c14 =
        new double[] {
          Double.NEGATIVE_INFINITY, -1, 0, 1e99, 0, Double.POSITIVE_INFINITY, Double.NaN, 0
        };
    assertThat(MutualInformationDC.mi(d3, c14, k, base)).isCloseTo(-0.1279, byLessThan(DELTA));
  }
}
