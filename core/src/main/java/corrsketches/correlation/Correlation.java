package corrsketches.correlation;

/** An interface for all correlation estimators implemented in this library. */
public interface Correlation {

  Estimate correlation(double[] x, double[] y);
}
