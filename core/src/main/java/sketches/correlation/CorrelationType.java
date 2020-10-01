package sketches.correlation;

import sketches.correlation.estimators.BootstrapedPearson;
import sketches.correlation.estimators.RinCorrelation;
import sketches.correlation.estimators.SpearmanCorrelation;

public enum CorrelationType {
  PEARSONS,
  SPEARMANS,
  RIN,
  ROBUST_QN,
  PM1_BOOTSTRAP;

  public static Correlation get(CorrelationType type) {
    switch (type) {
      case PEARSONS:
        return PearsonCorrelation::estimate;
      case ROBUST_QN:
        return Qn::estimate;
      case SPEARMANS:
        return SpearmanCorrelation::estimate;
      case RIN:
        return RinCorrelation::estimate;
      case PM1_BOOTSTRAP:
        return BootstrapedPearson::estimate;
      default:
        throw new IllegalArgumentException("Invalid correlation type: " + type);
    }
  }
}
