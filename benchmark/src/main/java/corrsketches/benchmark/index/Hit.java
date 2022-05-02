package corrsketches.benchmark.index;

import corrsketches.CorrelationSketch.ImmutableCorrelationSketch;
import corrsketches.correlation.Correlation.Estimate;
import java.io.IOException;
import java.util.Comparator;
import java.util.List;

public class Hit {

  public final String id;
  public final float score;

  protected final int docId;
  protected final SketchIndex index;
  protected final ImmutableCorrelationSketch query;
  protected ImmutableCorrelationSketch sketch;
  private Estimate correlation;
  private double rerankScore;

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

  @Override
  public String toString() {
    return String.format(
        "\nHit{\n\tid='%s'\n\tscore=%.3f\n\trerankScore=%.3f\n\tcorrelation=%s\n\tsketch=%s\n}",
        id, score, rerankScore, correlation != null ? correlation.coefficient : null, sketch);
  }

  public interface RerankStrategy {

    Comparator<Hit> RERANK_SCORE_DESC = (a, b) -> Double.compare(b.rerankScore, a.rerankScore);

    void sort(List<Hit> hits);
  }

  public static class CorrelationSketchReranker implements RerankStrategy {

    @Override
    public void sort(List<Hit> hits) {
      for (var hit : hits) {
        double corrAbs = Math.abs(hit.correlation());
        hit.rerankScore = Double.isNaN(corrAbs) ? 0.0 : corrAbs;
      }
      hits.sort(RERANK_SCORE_DESC);
    }
  }
}
