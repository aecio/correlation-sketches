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

  public Correlation get() {
    return get(this);
  }

  public static Correlation get(CorrelationType type) {
    switch (type) {
      case PEARSONS:
        return (NumericalCorrelation) PearsonCorrelation::estimate;
      case ROBUST_QN:
        return (NumericalCorrelation) QnCorrelation::estimate;
      case SPEARMANS:
        return (NumericalCorrelation) SpearmanCorrelation::estimate;
      case RIN:
        return (NumericalCorrelation) RinCorrelation::estimate;
      case PM1_BOOTSTRAP:
        return (NumericalCorrelation) BootstrapedPearson::estimate;
      case QCR:
        return (NumericalCorrelation) QCRCorrelation::estimate;
      case MUTUAL_INFORMATION:
        return MutualInformationMixed.INSTANCE;
      case NMI_SQRT:
        return (NumericalCorrelation) MutualInformation::estimateNmiSqrt; // FIXME
      case NMI_MAX:
        return (NumericalCorrelation) MutualInformation::estimateNmiMax; // FIXME
      case NMI_MIN:
        return (NumericalCorrelation) MutualInformation::estimateNmiMin; // FIXME
      default:
        throw new IllegalArgumentException("Invalid correlation type: " + type);
    }
  }
}
