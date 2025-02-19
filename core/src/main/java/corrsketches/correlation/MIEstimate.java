package corrsketches.correlation;

import corrsketches.statistics.Entropy;

public final class MIEstimate extends Estimate {

  /** probabilities of x */
  public final double[] px;

  /** probabilities of y */
  public final double[] py;

  /** entropy of y */
  public final double ey;

  /** entropy of x */
  public final double ex;

  public MIEstimate(double mi, int sampleSize, double[] px, double[] py) {
    super(mi, sampleSize);
    this.px = px;
    this.py = py;
    this.ex = Entropy.entropyFromProbs(px);
    this.ey = Entropy.entropyFromProbs(py);
  }

  public MIEstimate(double mi, int sampleSize, double ex, double ey) {
    super(mi, sampleSize);
    this.px = null;
    this.py = null;
    this.ex = ex;
    this.ey = ey;
  }

  /** @return The cardinality of variable X. */
  public int nx() {
    return px.length;
  }

  /** @return The cardinality of variable Y. */
  public int ny() {
    return py.length;
  }

  public double nmiMin() {
    return NMI.min(value, ex, ey);
  }

  public double nmiMax() {
    return NMI.max(value, ex, ey);
  }

  public double nmiSqrt() {
    return NMI.sqrt(value, ex, ey);
  }
}
