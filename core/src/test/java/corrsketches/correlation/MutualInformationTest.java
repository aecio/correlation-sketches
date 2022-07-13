package corrsketches.correlation;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.byLessThan;

import org.junit.jupiter.api.Test;

public class MutualInformationTest {

  public static final double DELTA = 0.00000001;

  @Test
  public void shouldComputeMutualInformation() {
    int[] x = new int[] {1, 1, 1, 2, 2, 2, 2, 2, 3, 3};
    int[] y = new int[] {1, 2, 2, 3, 2, 3, 2, 3, 1, 2};
    int[] z = new int[] {1, 2, 2, 1, 2, 3, 2, 3, 2, 2};

    assertThat(MutualInformation.score(x, y)).isCloseTo(0.3635634939595127, byLessThan(DELTA));
    assertThat(MutualInformation.score(x, z)).isCloseTo(0.23185620475171878, byLessThan(DELTA));
    assertThat(MutualInformation.score(y, z)).isCloseTo(0.6206868526328018, byLessThan(DELTA));
  }

  @Test
  public void shouldComputeCoOccurrenceMatrix() {
    int[] x = new int[] {1, 2, 3};
    int[] y = new int[] {1, 2, 3};

    int[][] com = MutualInformation.coOccurrenceMatrix(x, y, x.length);
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
