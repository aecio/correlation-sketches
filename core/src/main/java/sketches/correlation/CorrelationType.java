package sketches.correlation;

import sketches.correlation.estimators.RinCorrelation;
import sketches.correlation.estimators.SpearmanCorrelation;

public enum CorrelationType {
  PEARSONS,
  SPEARMANS,
  RIN,
  ROBUST_QN;

  public static Correlation get(CorrelationType type) {
    switch (type) {
      case PEARSONS:
        return PearsonCorrelation::coefficient;
      case ROBUST_QN:
        return Qn::correlation;
      case SPEARMANS:
        return SpearmanCorrelation::coefficient;
      case RIN:
        return RinCorrelation::coefficient;
      default:
        throw new IllegalArgumentException("Invalid correlation type: " + type);
    }
  }
}
