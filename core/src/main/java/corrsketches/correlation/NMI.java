package corrsketches.correlation;

/** Helper class with functions to help computes some normalized MI variants. */
public class NMI {

  public static double max(double mi, double ex, double ey) {
    return mi / Math.max(ex, ey);
  }

  public static double min(double mi, double ex, double ey) {
    return mi / Math.min(ex, ey);
  }

  public static double sqrt(double mi, double ex, double ey) {
    return mi / Math.sqrt(ex * ey);
  }

  public double infoGainRatioX(double mi, double ex) {
    return mi / ex;
  }

  public double infoGainRatioY(double mi, double ey) {
    return mi / ey;
  }
}
