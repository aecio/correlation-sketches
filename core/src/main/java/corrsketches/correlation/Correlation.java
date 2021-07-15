package corrsketches.correlation;

/** An interface for all correlation estimators implemented in this library. */
public interface Correlation {

  class Estimate {

    public final double coefficient;
    public final int sampleSize;

    public Estimate(final double coefficient, final int sampleSize) {
      this.coefficient = coefficient;
      this.sampleSize = sampleSize;
    }
  }

  Estimate correlation(double[] x, double[] y);
}
