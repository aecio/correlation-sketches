package corrsketches.correlation;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.byLessThan;

import org.junit.jupiter.api.Test;

public class MutualInformationMLETest {

  public static final double DELTA = 0.00000001;

  @Test
  public void shouldComputeMutualInformation() {
    int[] x = new int[] {1, 1, 1, 2, 2, 2, 2, 2, 3, 3};
    int[] y = new int[] {1, 2, 2, 3, 2, 3, 2, 3, 1, 2};
    int[] z = new int[] {1, 2, 2, 1, 2, 3, 2, 3, 2, 2};

    assertThat(MutualInformationMLE.mi(x, y).value)
        .isCloseTo(0.3635634939595127, byLessThan(DELTA));
    assertThat(MutualInformationMLE.mi(x, z).value)
        .isCloseTo(0.23185620475171878, byLessThan(DELTA));
    assertThat(MutualInformationMLE.mi(y, z).value)
        .isCloseTo(0.6206868526328018, byLessThan(DELTA));
  }

  @Test
  public void shouldComputeNormalizedMutualInformation() {
    int[] y = new int[] {1, 2, 2, 3, 2, 2, 3, 1, 2, 3, 2};
    int[] x = new int[] {2, 2, 2, 2, 2, 2, 2, 2, 3, 1, 2};

    final MIEstimate mi = MutualInformationMLE.mi(x, y);
    assertThat(mi.value).isCloseTo(0.1808106406067122, byLessThan(DELTA));
  }

  @Test
  public void shouldComputeNumberOfUniqueElementsInTheDomain() {
    int[] y = new int[] {1, 2, 2, 3, 4, 5, 3, 1, 2, 3, 2};
    int[] x = new int[] {2, 2, 2, 2, 2, 2, 2, 2, 3, 1, 2};

    final MIEstimate mi = MutualInformationMLE.mi(x, y);
    assertThat(mi.ny()).isEqualTo(5);
    assertThat(mi.nx()).isEqualTo(3);
  }

  @Test
  public void shouldComputeCoOccurrenceMatrix() {
    int[] x = new int[] {1, 2, 3};
    int[] y = new int[] {1, 2, 3};

    int[][] com = MutualInformationMLE.coOccurrenceMatrix(x, y, x.length);
    assertThat(com[0][0]).isEqualTo(1);
    assertThat(com[0][1]).isEqualTo(0);
    assertThat(com[0][2]).isEqualTo(0);
    assertThat(com[1][0]).isEqualTo(0);
    assertThat(com[1][1]).isEqualTo(1);
    assertThat(com[1][2]).isEqualTo(0);
    assertThat(com[2][0]).isEqualTo(0);
    assertThat(com[2][1]).isEqualTo(0);
    assertThat(com[2][2]).isEqualTo(1);
  }
}
