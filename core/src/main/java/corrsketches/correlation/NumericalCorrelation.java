package corrsketches.correlation;

/** An interface for all correlation estimators implemented in this library. */
public interface NumericalCorrelation extends Correlation {

  /**
   * Computes correlations for numerical variables.
   *
   * @return the correlation estimate of {@param x} and {@param y}.
   */
  Estimate correlation(double[] x, double[] y);

  @Override
  default Estimate ofNumerical(double[] x, double[] y) {
    return this.correlation(x, y);
  }
}
