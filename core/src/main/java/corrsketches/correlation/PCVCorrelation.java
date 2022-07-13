package corrsketches.correlation;

import com.google.common.base.Preconditions;
import corrsketches.statistics.Stats;
import smile.math.Math;

/**
 * Computes the correlation using the Principal Component Variances formula (same formula typically
 * used in robust correlation estimators.
 */
public class PCVCorrelation {

  private static final double SQRT_OF_TWO = Math.sqrt(2);

  public static Estimate estimate(double[] x, double[] y) {
    return new Estimate(correlation(x, y), x.length);
  }

  public static double correlation(double[] x, double[] y) {
    Preconditions.checkArgument(x.length == y.length, "x and y dimensions must match");

    double stdx = Stats.std(x);
    double stdy = Stats.std(y);
    // double meanx = Stats.mean(x);
    // double meany = Stats.mean(y);

    int n = y.length;
    double[] u = new double[n];
    double[] v = new double[n];

    for (int i = 0; i < n; i++) {
      // final double xstd = ( (x[i] - meanx) / stdx);
      // final double ystd = ( (y[i] - meany) / stdy);
      // u[i] = (xstd + ystd) / SQRT_OF_TWO;
      // v[i] = (xstd - ystd) / SQRT_OF_TWO;
      final double xstdsqrt2 = (x[i] / stdx) / SQRT_OF_TWO;
      final double ystdsqrt2 = (y[i] / stdy) / SQRT_OF_TWO;
      u[i] = xstdsqrt2 + ystdsqrt2;
      v[i] = xstdsqrt2 - ystdsqrt2;
    }

    double uscale = Stats.std(u);
    double vscale = Stats.std(v);

    double us2 = uscale * uscale;
    double vs2 = vscale * vscale;
    return (us2 - vs2) / (us2 + vs2);
  }
}
