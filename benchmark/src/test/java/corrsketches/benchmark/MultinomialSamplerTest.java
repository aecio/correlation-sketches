package corrsketches.benchmark;

import static org.assertj.core.api.Assertions.*;
import static org.assertj.core.data.Offset.offset;

import corrsketches.benchmark.distributions.MultinomialSampler;
import corrsketches.correlation.PearsonCorrelation;
import corrsketches.statistics.Stats;
import corrsketches.statistics.Variance;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.RepetitionInfo;

public class MultinomialSamplerTest {

  @RepeatedTest(500)
  public void shouldDrawSamplesFromMultinomialDistribution() {
    // given
    double delta = 15;
    int n = 1000;
    // double delta = 0.2;
    // int n = 15;
    double p = 0.55;
    double q = 0.45;
    int length = 10000;

    // when
    double[] probs = new double[] {p, q, 1 - (p + q)};
    // var sampler = new SimpleMultinomialSampler(ThreadLocalRandom.current(), probs);
    var sampler = new MultinomialSampler(ThreadLocalRandom.current(), n, probs);
    double[][] data = sampler.sample(length);

    assertThat(data.length).isEqualTo(probs.length);

    assertThat(data[0].length).isEqualTo(length);
    assertThat(data[1].length).isEqualTo(length);
    assertThat(data[2].length).isEqualTo(length);

    assertThat(Variance.uvar(data[0])).isEqualTo(n * p * (1. - p), offset(delta));
    assertThat(Variance.uvar(data[1])).isEqualTo(n * q * (1. - q), offset(delta));

    double expectedCovPQ = -n * p * q;
    assertThat(Stats.cov(data[0], data[1])).isEqualTo(expectedCovPQ, offset(delta));

    double expectedCorrPQ = -p * q / Math.sqrt(p * (1 - p) * q * (1 - q));
    double deltaCorr = 0.05;
    assertThat(PearsonCorrelation.coefficient(data[0], data[1]))
        .isEqualTo(expectedCorrPQ, offset(deltaCorr));
  }

  @RepeatedTest(500)
  public void shouldDrawSamplesFromMultinomialDistributionWithRandomParameters(
      RepetitionInfo repetitionInfo) {
    // given
    Random rng = new Random(repetitionInfo.getCurrentRepetition());
    double[] probs =
        Stats.toProbabilities(new double[] {rng.nextDouble(), rng.nextDouble(), rng.nextDouble()});
    int n = 10 + rng.nextInt(1000);
    double p = probs[0];
    double q = probs[1];
    int length = 10000;
    double delta = n / 10;

    // when
    // var sampler = new SimpleMultinomialSampler(ThreadLocalRandom.current(), probs);
    var sampler = new MultinomialSampler(ThreadLocalRandom.current(), n, probs);
    double[][] data = sampler.sample(length);

    assertThat(data.length).isEqualTo(probs.length);

    assertThat(data[0].length).isEqualTo(length);
    assertThat(Variance.uvar(data[0])).isEqualTo(n * p * (1. - p), offset(delta));

    assertThat(data[1].length).isEqualTo(length);
    assertThat(Variance.uvar(data[1])).isEqualTo(n * q * (1. - q), offset(delta));

    double expectedCovPQ = -n * p * q;
    assertThat(Stats.cov(data[0], data[1])).isEqualTo(expectedCovPQ, offset(delta));

    double meanDelta = 0.6;
    assertThat(Stats.mean(data[0])).isEqualTo(n * probs[0], offset(meanDelta));
    assertThat(Stats.mean(data[1])).isEqualTo(n * probs[1], offset(meanDelta));
    assertThat(Stats.mean(data[2])).isEqualTo(n * probs[2], offset(meanDelta));

    double expectedCorrPQ = -p * q / Math.sqrt(p * (1 - p) * q * (1 - q));
    double deltaCorr = 0.05;
    assertThat(PearsonCorrelation.coefficient(data[0], data[1]))
        .isEqualTo(expectedCorrPQ, offset(deltaCorr));
  }

  //  @RepeatedTest(500)
  //  public void shouldDrawSamplesFromBinomial(RepetitionInfo repetitionInfo) {
  //    Random rng = new Random(repetitionInfo.getCurrentRepetition() + 1234);
  //    int n = 10 + rng.nextInt(1000);
  //    double p = rng.nextDouble();
  //    int bswt = BinomialSampler.binomialSWT(new Random(repetitionInfo.getCurrentRepetition()), n,
  // p);
  //    int getb = new BinomialSampler(new
  // Random(repetitionInfo.getCurrentRepetition())).getBinomial(n, p);
  //    assertThat(getb).isEqualTo(bswt);
  //  }
}
