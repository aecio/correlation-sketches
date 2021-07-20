package corrsketches.benchmark.index;

import corrsketches.CorrelationSketch.ImmutableCorrelationSketch;
import corrsketches.correlation.Correlation.Estimate;
import corrsketches.correlation.CorrelationType;
import java.io.IOException;

public class Hit {

  public final String id;
  public final float score;

  private final int docId;
  private final SketchIndex index;
  private final ImmutableCorrelationSketch query;
  private ImmutableCorrelationSketch hit;
  private Estimate correlation;

  public Hit(
      String id,
      ImmutableCorrelationSketch query,
      ImmutableCorrelationSketch sketch,
      float score,
      int docId,
      SketchIndex index) {
    this.id = id;
    this.query = query;
    this.hit = sketch;
    this.score = score;
    this.docId = docId;
    this.index = index;
  }

  public double correlation() {
    if (this.correlation == null) {
      if (hit == null) {
        try {
          this.hit = this.index.loadSketch(docId);
        } catch (IOException e) {
          throw new RuntimeException("Failed to load sketch from index", e);
        }
      }
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
