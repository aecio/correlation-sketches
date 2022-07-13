package corrsketches.correlation;

import corrsketches.Column;

/** An interface for all correlation estimators implemented in this library. */
public interface Correlation {

  /**
   * Computes correlations for numerical variables.
   *
   * @return the correlation estimate of {@param x} and {@param y}.
   */
  Estimate correlation(double[] x, double[] y);

  default Estimate correlation(Column x, Column y) {
    return correlation(x.values, y.values);
  }
}
