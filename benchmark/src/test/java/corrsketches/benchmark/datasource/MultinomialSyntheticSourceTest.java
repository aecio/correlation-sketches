package corrsketches.benchmark.datasource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.data.Offset.offset;

import corrsketches.benchmark.datasource.MultinomialSyntheticSource.MultinomialParameters;
import java.util.Random;
import org.junit.jupiter.api.Test;

public class MultinomialSyntheticSourceTest {
  @Test
  public void shouldComputeAnalyticalMIFromParameters() {
    int n = 1000;
    double p = 0.35;
    double q = 0.6;
    var mnlParams = new MultinomialParameters(n, p, q);
    assertThat(mnlParams.getMutualInformation())
        .isEqualTo(mnlParams.getBivariateMI(), offset(0.005));
  }

  @Test
  public void compareDistributionMiToBivariateApproximation() {
    int n = 512;
    Random rng = new Random();
    int miCount = 0;
    int miBivCount = 0;
    double diff = 0;

    int total = 100;
    for (int i = 0; i < total; i++) {
      MultinomialParameters parameters =
          MultinomialSyntheticSource.createMultinomialParameters(rng, n);
      double mi = parameters.getMutualInformation();
      double miBiv = parameters.getBivariateMI();

      if (mi > 2.5) {
        miCount++;
        System.out.printf("MI: " + mi);
      }
      if (miBiv > 2.5) {
        miBivCount++;
        System.out.printf("  Biv:" + miBiv);
      }
      if (mi > 2.5 || miBiv > 2.5) {
        System.out.println();
      }
      diff += (mi - miBiv);
    }
    double averageDiff = diff / total;
    System.out.printf(
        "mi: %d miBiv %d diff: %d avg_diff: %.3f\n",
        miCount, miBivCount, miCount - miBivCount, averageDiff);
    assertThat(averageDiff).isLessThan(0.1);
  }
}
