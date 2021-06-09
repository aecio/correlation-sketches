package corrsketches.benchmark.index;

import corrsketches.CorrelationSketch.ImmutableCorrelationSketch;
import corrsketches.correlation.Correlation.Estimate;
import corrsketches.correlation.CorrelationType;

public class Hit {

  public final String id;
  public final float score;
  private final ImmutableCorrelationSketch query;
  private final ImmutableCorrelationSketch hit;
  private Estimate correlation;

  public Hit(
      String id, ImmutableCorrelationSketch query, ImmutableCorrelationSketch sketch, float score) {
    this.id = id;
    this.query = query;
    this.hit = sketch;
    this.score = score;
  }

  public double correlation() {
    if (this.correlation == null) {
      this.correlation = query.correlationTo(hit);
    }
    return correlation.coefficient;
  }

  public double correlationAbsolute() {
    return Math.abs(correlation());
  }

  public double robustCorrelation() {
    return query.correlationTo(hit, CorrelationType.get(CorrelationType.ROBUST_QN)).coefficient;
  }

  public double qcrCorrelation() {
    return query.correlationTo(hit, CorrelationType.get(CorrelationType.QCR)).coefficient;
  }
}
