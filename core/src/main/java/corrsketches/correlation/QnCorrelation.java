package corrsketches.correlation;

import com.google.common.base.Preconditions;
import corrsketches.correlation.Correlation.Estimate;
import corrsketches.statistics.Qn;

/**
 * Implements the robust correlation based on principal component variances described in
 * "Shevlyakov, G.L. and Oja, H., 2016. Robust correlation: Theory and applications (Vol. 3). John
 * Wiley &amp; Sons". This implementation uses the robust Qn scale estimator proposed in Peter J.
 * Rousseeuw and Christophe Croux (1993).
 */
public class QnCorrelation {

  private static final double SQRT_OF_TWO = Math.sqrt(2);

  public static Estimate estimate(double[] x, double[] y) {
    return new Estimate(correlation(x, y), x.length);
  }

  public static double correlation(double[] x, double[] y) {
    Preconditions.checkArgument(x.length == y.length, "x and y dimensions must match");

    double stdx = Qn.estimateScale(x);
    double stdy = Qn.estimateScale(y);

    int n = y.length;
    double[] u = new double[n];
    double[] v = new double[n];

    for (int i = 0; i < n; i++) {
      final double xstdsqrt2 = (x[i] / stdx) / SQRT_OF_TWO;
      final double ystdsqrt2 = (y[i] / stdy) / SQRT_OF_TWO;
      u[i] = xstdsqrt2 + ystdsqrt2;
      v[i] = xstdsqrt2 - ystdsqrt2;
    }

    double uscale = Qn.estimateScale(u);
    double vscale = Qn.estimateScale(v);

    double us2 = uscale * uscale;
    double vs2 = vscale * vscale;

    return (us2 - vs2) / (us2 + vs2);
  }
}
