package sketches.correlation.estimators;

import java.util.Arrays;
import java.util.Random;
import sketches.correlation.Correlation.Estimate;
import sketches.correlation.PearsonCorrelation;
import sketches.statistics.Stats;
import smile.math.Math;

public class BootstrapedPearson {

//  public static CorrelationEstimate coefficient(double[] x, double[] y) {
//    Random random = new Random(0);
//
//    int N = y.length;
//    int B = 100;
//    double[] estimates = new double[B];
//
//    double corr = PearsonCorrelation.coefficient(x, y);
//    double corr_abs = Math.abs(corr);
//
//    final double alpha = 0.05; // must be > 0 and <= 1
//    final double p0 = alpha / 2;
//    final int a = 5;
//    double theta = 0.4;
//    double c = (1 + theta) * p0 / (1. - p0);
//
//    double[] sy = new double[N];
//    double[] sx = new double[N];
//    int ind = 0;
//    for (int i = 0; i < B; i++) {
//      // re-sample of vectors x and y creating sx and sy
//      resample(x, y, random, N, sy, sx);
//
//      // compute correlation estimate on bootstrapped sample
//      double sr = PearsonCorrelation.coefficient(sx, sy);
//      if (Double.isNaN(sr)) {
//        estimates[i] = 0.0;
//      } else {
//        estimates[i] = sr;
//      }
//
//      // compute statistics for early termination
//      double srabs = Math.abs(sr);
//
////      boolean h0 = srabs > TINY;
//      boolean h0 = srabs < corr_abs;
//      if (h0) {
//        ind++;
//      }
//
//      final int k = i + 1;
//      double currP = (1.0 / (double) k) * ind;
//      if (currP > (a / k + c) / (1 + c)) {
////        System.out.printf("broke loop at k=%d p=%.3f\n", k, currP);
//        B = k;
//        break;
//      }
//    }
//    Arrays.sort(estimates, 0, B);
//
//    return createCorrelationEstimate(B, estimates, corr, alpha);
//  }
//
//  private static CI createPercentileIntervals(int b, double[] estimates,
//      double corr, double alpha) {
//    double percentile = alpha / 2;
//
//    int idxLb = (int) Math.ceil((percentile) * b);
//    int idxUb = (int) Math.ceil((1. - percentile) * b);
//    double lb = estimates[idxLb - 1];
//    double ub = estimates[idxUb - 1];
//    double corrMedian = estimates[((int) Math.ceil(0.5 * b)) - 1];
//    double corrMean = Stats.mean(estimates);
//    return new CI(corr, corrMean, corrMedian, lb, ub);
//  }

  public static double coefficient(double[] x, double[] y) {
    return estimate(x, y).corrBsMean;
  }

  public static BootstrapEstimate estimate(double[] x, double[] y) {
    Random random = new Random(0);

    final int n = y.length;

    int B = 10000;
    double[] estimates = new double[B];

    final double corr = PearsonCorrelation.coefficient(x, y);

    double mean = 0.0;
    int ind = 0;

    double[] sy = new double[n];
    double[] sx = new double[n];
    for (int i = 0; i < B; i++) {
      resample(x, y, random, n, sy, sx);
      // compute correlation estimate on bootstrapped sample
      double sr = PearsonCorrelation.coefficient(sx, sy);
      if (Double.isNaN(sr)) {
        estimates[i] = 0.0;
      } else {
        estimates[i] = sr;
      }

      // updated current mean
      final int count = i + 1;
      final double diff = (estimates[i] - mean) / count;
      mean = mean + diff;

      // evaluate early termination
      final double absDiff = Math.abs(diff);
      if (absDiff > 0.01) {
        ind++;
      }
      final double p = ind / (double) count;
      if (i >= 5 && p < 0.05) {
        B = count;
        break;
      }
    }
    Arrays.sort(estimates, 0, B);

    return createPM1ConfidenceInterval(B, estimates, n, corr);
  }

  /**
   * Creates confidence interval using the modified percentiles (PM1) as described by Bishara et.
   * al. in "Asymptotic confidence intervals for the Pearson correlation via skewness and kurtosis",
   * and originally proposed by Rand R. Wilcox from the paper "Confidence intervals for the slope of
   * a regression line when the error term has nonconstant variance".
   *
   * PM1 adjusts the percentiles based on sample size.
   */
  private static BootstrapEstimate createPM1ConfidenceInterval(int B, double[] estimates, int n,
      double corr) {
    double a, c;
    if (n < 40) {
      // n < 40
      a = 7;
      c = 593;
    } else if (n < 80) {
      // 40 <= n < 80
      a = 8;
      c = 592;
    } else if (n < 180) {
      // 80 <= n < 180
      a = 11;
      c = 588;
    } else if (n < 250) {
      // 180 <= n < 250
      a = 14;
      c = 585;
    } else {
      // >= 250
      a = 15;
      c = 584;
    }
    int idxLb = (int) Math.ceil((B / 599.) * a);
    int idxUb = (int) Math.ceil((B / 599.) * c);
    double lb = estimates[idxLb - 1];
    double ub = estimates[idxUb - 1];
    double corrMedian = estimates[((int) Math.ceil(0.5 * B)) - 1];
    double corrMean = Stats.mean(estimates, B);
    return new BootstrapEstimate(n, corr, corrMean, corrMedian, lb, ub);
  }

  /**
   * Creates a sample with replacement of the vectors x and y and writes the output to sx and sy.
   * Output is written to sx and sy to avoid creating multiple instances of the array.
   */
  private static void resample(double[] x, double[] y, Random random, int n,
      double[] sy, double[] sx) {
    for (int j = 0; j < n; j++) {
      int ni = random.nextInt(n);
      sx[j] = x[ni];
      sy[j] = y[ni];
    }
  }

  public static class BootstrapEstimate extends Estimate {

    public final double corrEst;
    public final double corrBsMean;
    public final double corrBsMedian;
    public final double lowerBound;
    public final double upperBound;

    public BootstrapEstimate(int sampleSize, double corrEst, double corrMean, double corrMedian,
        double lowerBound, double upperBound) {
      super(corrMean, sampleSize);
      this.corrEst = corrEst;
      this.corrBsMean = corrMean;
      this.corrBsMedian = corrMedian;
      this.lowerBound = lowerBound;
      this.upperBound = upperBound;
    }

    @Override
    public String toString() {
      return String
          .format("\n[r=%+.3f, r_mean:=%.3f r_median=%.3f lb=%+.3f, ub=%+.3f]", corrEst, corrBsMean,
              corrBsMedian, lowerBound, upperBound);
    }
  }

}
