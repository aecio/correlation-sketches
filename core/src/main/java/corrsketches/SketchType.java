package corrsketches;

public enum SketchType {
  KMV,
  GKMV;

  public static SketchOptions parseOptions(SketchType type, String value) {
    final SketchOptions options;
    switch (type) {
      case KMV:
        options = new KMVOptions(Integer.parseInt(value.trim()));
        break;
      case GKMV:
        options = new GKMVOptions(Double.parseDouble(value.trim()));
        break;
      default:
        throw new IllegalArgumentException("Unsupported sketch type: " + type);
    }
    return options;
  }

  public static class SketchOptions {

    public final SketchType type;

    protected SketchOptions(SketchType type) {
      this.type = type;
    }
  }

  public static class KMVOptions extends SketchOptions {

    public final int k;

    public KMVOptions(int k) {
      super(SketchType.KMV);
      this.k = k;
    }
  }

  public static class GKMVOptions extends SketchOptions {

    public final double t;

    public GKMVOptions(double t) {
      super(SketchType.GKMV);
      this.t = t;
    }
  }
}
