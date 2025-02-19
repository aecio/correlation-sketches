package corrsketches.statistics;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

public class EntropyTest {

  @Test
  public void testCategoricalEntropy() {
    int[] x;
    x = new int[] {1};
    assertThat(Entropy.entropy(x)).isEqualTo(0.0);

    x = new int[] {1, 1, 1, 1, 1};
    assertThat(Entropy.entropy(x)).isEqualTo(0.0);

    x = new int[] {1, 1, 2, 2, 1};
    assertThat(Entropy.entropy(x)).isEqualTo(0.6730116670092565);

    x = new int[] {1, 2, 4, 8, 16};
    assertThat(Entropy.entropy(x)).isEqualTo(1.6094379124341005);

    x = new int[] {1, 2};
    assertThat(Entropy.entropy(x)).isEqualTo(0.6931471805599453);
  }
}
