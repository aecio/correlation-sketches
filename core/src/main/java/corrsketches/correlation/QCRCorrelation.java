package corrsketches.correlation;

import corrsketches.statistics.Stats;

/** Implements Quadrant Count Ratio (QCR) correlation. */
public class QCRCorrelation {

  public static Estimate estimate(double[] x, double[] y) {
    return new Estimate(coefficient(x, y), x.length);
  }

  public static double coefficient(double[] x, double[] y) {
    final int n = x.length;
    final double mx = Stats.mean(x);
    final double my = Stats.mean(y);

    // double stdx = Stats.std(x);
    // double stdy = Stats.std(x);
    // double[] ux = new double[n];
    // for (int i = 0; i < ux.length; i++) {
    //   ux[i] = (x[i] - mx) / stdx;
    // }
    // double[] uy = new double[y.length];
    // for (int i = 0; i < uy.length; i++) {
    //   uy[i] = (y[i] - my) / stdy;
    // }
    // x = ux;
    // y = uy;
    // mx = 0;
    // my = 0;

    int q1 = 0, q2 = 0, q3 = 0, q4 = 0;
    for (int i = 0; i < n; i++) {
      // points that lie on the division lines (x=0, y=0) are ignored
      if (x[i] > mx) {
        if (y[i] > my) {
          q1++;
        } else if (y[i] < my) {
          q4++;
        }
      } else if (x[i] < mx) {
        if (y[i] > my) {
          q2++;
        } else if (y[i] < my) {
          q3++;
        }
      }
    }
    // System.out.printf("q1=%d q3=%d q2=%d q4=%d n=%d\n", q1, q3, q2, q4, n);
    final double q = (q1 + q3 - q2 - q4) / (double) n;
    return q;
  }
}
