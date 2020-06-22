package sketches.correlation.estimators;

import smile.math.Math;

public class SpearmanCorrelation {

  public static double coefficient(double[] x, double[] y) {
    return Math.spearman(x, y);
  }

}
