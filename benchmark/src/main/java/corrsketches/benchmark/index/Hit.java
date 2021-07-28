package corrsketches.benchmark.index;

import corrsketches.CorrelationSketch.ImmutableCorrelationSketch;
import corrsketches.correlation.Correlation.Estimate;
import java.io.IOException;

public class Hit {

  public final String id;
  public final float score;

  protected final int docId;
  protected final SketchIndex index;
  protected final ImmutableCorrelationSketch query;
  protected ImmutableCorrelationSketch sketch;
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
    this.sketch = sketch;
    this.score = score;
    this.docId = docId;
    this.index = index;
  }

  public double correlation() {
    if (this.correlation == null) {
      if (sketch == null) {
        try {
          this.sketch = this.index.loadSketch(docId);
        } catch (IOException e) {
          throw new RuntimeException("Failed to load sketch from index", e);
        }
      }
      this.correlation = query.correlationTo(sketch);
    }
    return correlation.coefficient;
  }

  public double correlationAbsolute() {
    return Math.abs(correlation());
  }

  @Override
  public String toString() {
    return String.format(
        "\nHit{\n\tid='%s'\n\tscore=%.3f\n\tcorrelation=%s\n}",
        id, score, correlation != null ? correlation.coefficient : null);
  }
}
