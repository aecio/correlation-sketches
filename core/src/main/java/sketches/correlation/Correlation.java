package sketches.correlation;

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
