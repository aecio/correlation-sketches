package corrsketches.correlation;

public enum CorrelationType {
  PEARSONS,
  SPEARMANS,
  RIN,
  ROBUST_QN,
  PM1_BOOTSTRAP,
  QCR;

  public static Correlation get(CorrelationType type) {
    switch (type) {
      case PEARSONS:
        return PearsonCorrelation::estimate;
      case ROBUST_QN:
        return QnCorrelation::estimate;
      case SPEARMANS:
        return SpearmanCorrelation::estimate;
      case RIN:
        return RinCorrelation::estimate;
      case PM1_BOOTSTRAP:
        return BootstrapedPearson::estimate;
      case QCR:
        return QCRCorrelation::estimate;
      default:
        throw new IllegalArgumentException("Invalid correlation type: " + type);
    }
  }
}
