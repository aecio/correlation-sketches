package corrsketches.util;

import corrsketches.statistics.Stats;
import java.util.Arrays;

public class RandomArrays {

  public static CI percentiles(long[] x, double alpha) {
    double[] y = new double[x.length];
    for (int i = 0; i < x.length; i++) {
      y[i] = x[i];
    }
    return percentiles(y, alpha);
  }

  public static CI percentiles(double[] x, double alpha) {
    Arrays.sort(x);
    double percentile = alpha / 2.0;
    int idxLb = (int) Math.ceil((percentile) * x.length);
    int idxUb = (int) Math.ceil((1. - percentile) * x.length);
    double lb = x[idxLb - 1];
    double ub = x[idxUb - 1];
    final double mean = Stats.mean(x);
    return new CI(mean, lb, ub);
  }

  public static class CI {

    public final double mean;
    public final double ub;
    public final double lb;

    public CI(double mean, double lb, double ub) {
      this.mean = mean;
      this.lb = lb;
      this.ub = ub;
    }
  }
}
