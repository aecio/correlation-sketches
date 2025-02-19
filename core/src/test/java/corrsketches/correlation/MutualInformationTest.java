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

    assertThat(MutualInformation.ofCategorical(x, y).value)
        .isCloseTo(0.3635634939595127, byLessThan(DELTA));
    assertThat(MutualInformation.ofCategorical(x, z).value)
        .isCloseTo(0.23185620475171878, byLessThan(DELTA));
    assertThat(MutualInformation.ofCategorical(y, z).value)
        .isCloseTo(0.6206868526328018, byLessThan(DELTA));
  }

  @Test
  public void shouldComputeNormalizedMutualInformation() {
    int[] y = new int[] {1, 2, 2, 3, 2, 2, 3, 1, 2, 3, 2};
    int[] x = new int[] {2, 2, 2, 2, 2, 2, 2, 2, 3, 1, 2};

    final MIEstimate mi = MutualInformation.ofCategorical(x, y);
    assertThat(mi.value).isCloseTo(0.1808106406067122, byLessThan(DELTA));
    assertThat(mi.ex).isCloseTo(0.6001660731596457, byLessThan(DELTA));
    assertThat(mi.ey).isCloseTo(0.9949236325717751, byLessThan(DELTA));
    assertThat(mi.nmiMin()).isCloseTo(0.3012676802185986, byLessThan(DELTA));
    assertThat(mi.nmiMax()).isCloseTo(0.18173318502781496, byLessThan(DELTA));
    assertThat(mi.nmiSqrt()).isCloseTo(0.23398789514004176, byLessThan(DELTA));
  }
}
