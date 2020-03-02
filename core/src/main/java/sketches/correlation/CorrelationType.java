package sketches.correlation;

public enum CorrelationType {
  PEARSONS,
  ROBUST_QN;

  public static Correlation get(CorrelationType type) {
    switch (type) {
      case PEARSONS:
        return PearsonCorrelation::coefficient;
      case ROBUST_QN:
        return Qn::correlation;
      default:
        throw new IllegalArgumentException("Invalid correlation type: " + type);
    }
  }
}
