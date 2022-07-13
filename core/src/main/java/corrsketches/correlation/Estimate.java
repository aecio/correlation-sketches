package corrsketches.correlation;

public class Estimate {

  public final double value;
  public final int sampleSize;

  public Estimate(final double value, final int sampleSize) {
    this.value = value;
    this.sampleSize = sampleSize;
  }

  @Override
  public String toString() {
    return "Estimate{" + "value=" + value + ", sampleSize=" + sampleSize + '}';
  }
}
