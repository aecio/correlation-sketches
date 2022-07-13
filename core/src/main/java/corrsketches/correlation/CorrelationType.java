package corrsketches.correlation;

public enum CorrelationType {

  // for numerical data
  PEARSONS,
  SPEARMANS,
  RIN,
  ROBUST_QN,
  PM1_BOOTSTRAP,
  QCR,
  // for categorical data
  MUTUAL_INFORMATION,
  NMI_SQRT,
  NMI_MAX,
  NMI_MIN;

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
      case MUTUAL_INFORMATION:
        return MutualInformation::estimateMi;
      case NMI_SQRT:
        return MutualInformation::estimateNmiSqrt;
      case NMI_MAX:
        return MutualInformation::estimateNmiMax;
      case NMI_MIN:
        return MutualInformation::estimateNmiMin;
      default:
        throw new IllegalArgumentException("Invalid correlation type: " + type);
    }
  }
}
