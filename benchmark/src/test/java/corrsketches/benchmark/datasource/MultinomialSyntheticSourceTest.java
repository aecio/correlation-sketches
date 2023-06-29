package corrsketches.benchmark.datasource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.data.Offset.offset;

import org.junit.jupiter.api.Test;

public class MultinomialSyntheticSourceTest {
  @Test
  public void shouldComputeAnalyticalMIFromParameters() {
    int n = 1000;
    double p = 0.35;
    double q = 0.6;
    var mnlParams = new MultinomialSyntheticSource.MultinomialParameters(n, p, q);
    assertThat(mnlParams.getMutualInformation())
        .isEqualTo(mnlParams.getBivariateMI(), offset(0.005));
  }
}
