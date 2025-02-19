package corrsketches.benchmark;

import static corrsketches.benchmark.utils.LogFactorial.logOfFactorial;
import static org.assertj.core.api.Assertions.*;
import static org.assertj.core.data.Offset.offset;

import com.google.common.math.BigIntegerMath;
import corrsketches.Column;
import corrsketches.benchmark.distributions.MultinomialSampler;
import corrsketches.correlation.*;
import corrsketches.statistics.Stats;
import corrsketches.statistics.Variance;
import java.math.BigDecimal;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.RepetitionInfo;

public class MultinomialSamplerTest {

  @RepeatedTest(10)
  public void shouldDrawSamplesFromMultinomialDistribution() {
    // given
    double delta = 15;
    int n = 1000;
    // double delta = 0.2;
    // int n = 15;
    double p = 0.55;
    double q = 0.44;
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

  @RepeatedTest(10)
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
    double delta = n / 10d;

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

    double expectedMiPQ = -0.5 * Math.log(1.0 - Math.pow(expectedCorrPQ, 2));
    double deltaMi = 0.285;
    assertThat(MutualInformationMixedKSG.mi(data[0], data[1]))
        .isEqualTo(expectedMiPQ, offset(deltaMi));
    assertThat(MutualInformationMixedKSG.miWithNoise(data[0], data[1]))
        .isEqualTo(expectedMiPQ, offset(deltaMi));
    assertThat(MutualInformationDC.mi(Column.castToIntArray(data[0]), data[1]))
        .isEqualTo(expectedMiPQ, offset(deltaMi));
    // // The MI estimator for categorical data tends to overestimate the true value
    // assertThat(MutualInformation.ofCategorical(
    //        Column.castToIntArray(data[0]),
    //        Column.castToIntArray(data[1])).value
    // ).isEqualTo(expectedMiPQ, offset(deltaMi));
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

  public static void main(String[] args) {

    int m = 32; // number of trials
    double p1 = .03; // event probability p1
    double p2 = .925; // event probability p2

    double result = calcTrinomialMI(m, p1, p2);
    System.out.println(result);
  }

  public static double calcTrinomialMI(int m, double p1, double p2) {

    final double logOfFactorialOfM = logOfFactorial(m);

    double p3 = 1.0 - (p1 + p2);
    double jointEntropy = 0.0;
    jointEntropy -= logOfFactorialOfM;
    jointEntropy -= m * (p1 * Math.log(p1) + p2 * Math.log(p2) + p3 * Math.log(p3));
    jointEntropy += sum(m, p1);
    jointEntropy += sum(m, p2);
    jointEntropy += sum(m, p3);

    double px1 = p1;
    double px2 = 1.0 - p1;
    double xEntropy = 0.0;
    xEntropy -= logOfFactorialOfM;
    xEntropy -= m * (px1 * Math.log(px1) + px2 * Math.log(px2));
    xEntropy += sum(m, px1);
    xEntropy += sum(m, px2);

    double py1 = p2;
    double py2 = 1.0 - p2;
    double yEntropy = 0.0;
    yEntropy -= logOfFactorialOfM;
    yEntropy -= m * (py1 * Math.log(py1) + py2 * Math.log(py2));
    yEntropy += sum(m, py1);
    yEntropy += sum(m, py2);

    return xEntropy + yEntropy - jointEntropy;
  }

  public static double sum(int m, double p) {
    double sum = 0.0;
    for (int i = 0; i <= m; i++) {
      final BigDecimal comb = new BigDecimal(BigIntegerMath.binomial(m, i));
      sum +=
          comb.multiply(
                  BigDecimal.valueOf(Math.pow(p, i) * Math.pow(1 - p, m - i) * logOfFactorial(i)))
              .doubleValue();
    }
    return sum;
  }
}
