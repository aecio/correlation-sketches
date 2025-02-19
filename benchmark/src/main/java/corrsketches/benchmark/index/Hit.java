package corrsketches.benchmark.index;

import corrsketches.CorrelationSketch.ImmutableCorrelationSketch;
import corrsketches.correlation.Estimate;
import java.io.IOException;
import java.util.*;

public class Hit {

  public final String id;
  public final float score;

  protected final int docId;
  protected final SketchIndex index;
  protected final ImmutableCorrelationSketch query;
  protected ImmutableCorrelationSketch sketch;
  private Estimate correlation;
  public double rerankScore;
  private double joinability = -1;

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
      this.correlation = query.correlationTo(this.sketch());
    }
    return correlation.value;
  }

  public double joinability() {
    if (this.joinability < 0) {
      this.joinability = query.containment(this.sketch());
    }
    return this.joinability;
  }

  public ImmutableCorrelationSketch sketch() {
    if (sketch == null) {
      try {
        this.sketch = this.index.loadSketch(docId);
      } catch (IOException e) {
        throw new RuntimeException("Failed to load sketch from index", e);
      }
    }
    return sketch;
  }

  @Override
  public String toString() {
    return String.format(
        "\nHit{\n\tid='%s'\n\tscore=%.3f\n\trerankScore=%.3f\n\tcorrelation=%s\n\tsketch=%s\n}",
        id, score, rerankScore, correlation != null ? correlation.value : null, sketch);
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

  public static class Corr1Join1SketchReranker implements RerankStrategy {

    @Override
    public void sort(List<Hit> hits) {
      for (var hit : hits) {
        var joinability = hit.joinability();
        var correlation = hit.correlation();
        double corrAbs = Math.abs(correlation);
        double corrScore = Double.isNaN(corrAbs) ? 0.0 : corrAbs;
        hit.rerankScore = corrScore * joinability;
      }
      hits.sort(RERANK_SCORE_DESC);
    }
  }

  public static class GreedyDiverseSketchReranker implements RerankStrategy {
    @Override
    public void sort(List<Hit> hits) {
      var candidates = new ArrayList<>(hits);
      List<Hit> selected = new ArrayList<>();
      while (!candidates.isEmpty()) {
        System.out.println("cand size: " + candidates.size());
        double maxScore = -Double.MAX_VALUE;
        int best = 0;
        for (int i = 0; i < candidates.size(); i++) {
          var cand = candidates.get(i);
          var correlation = computeAbsCorr(cand.query, cand.sketch);
          var maxAbsCorr = findMaxAbsCorr(cand, selected);
          var corrScore = correlation - maxAbsCorr;
          var score = corrScore * cand.joinability();
          if (score > maxScore) {
            maxScore = score;
            best = i;
            cand.rerankScore = score;
            System.out.println("score = " + score);
            System.out.println("maxAbsCorr = " + maxAbsCorr);
            System.out.println("correlation = " + correlation);
            System.out.println("rerankScore = " + cand.rerankScore);
            System.out.println();
          }
        }

        selected.add(candidates.get(best));
        candidates.remove(best);
      }
      hits.clear();
      hits.addAll(selected);
    }
  }

  public static class GreedyDiverse3SketchReranker implements RerankStrategy {
    @Override
    public void sort(List<Hit> candidates) {
      List<Hit> selected = new ArrayList<>(candidates.size());
      int[] selectedIdx = new int[candidates.size()];
      int rank = 1;
      while (rank <= candidates.size()) {
        System.out.println("cand size: " + rank);
        double maxScore = -Double.MAX_VALUE;
        int bestIdx = 0;
        Hit bestCandidate = null;
        for (int i = 0; i < candidates.size(); i++) {
          if (selectedIdx[i] > 0) { // skip items already selected
            continue;
          }
          var cand = candidates.get(i);
          var correlation = computeAbsCorr(cand.query, cand.sketch);
          var maxAbsCorr = findMaxAbsCorr(cand, selected);
          var corrScore = Math.max(0.01, correlation - maxAbsCorr);
          var score = corrScore * cand.joinability();
          if (score > maxScore) {
            maxScore = score;
            bestIdx = i;
            bestCandidate = cand;
            cand.rerankScore = score;
            //            System.out.println("id = " + cand.id + "  i=" + i);
            //            System.out.println("score = " + cand.score);
            //            System.out.println("maxAbsCorr = " + maxAbsCorr);
            //            System.out.println("correlation = " + correlation);
            //            System.out.println("corrScore   = " + corrScore);
            //            System.out.println("joinability = " + cand.joinability);
            //            System.out.printf("rerankScore = %.10f\n", cand.rerankScore);
            //            System.out.println();
          }
        }
        selectedIdx[bestIdx] = rank++;
        selected.add(bestCandidate);
      }
      candidates.sort(RERANK_SCORE_DESC);
    }
  }

  public static class GreedyDiverse2SketchReranker implements RerankStrategy {

    private final int topN = 25;

    @Override
    public void sort(List<Hit> candidates) {
      List<Hit> selected = new ArrayList<>(candidates.size());
      int[] selectedIdx = new int[candidates.size()];
      int rank = 1;
      int limit = Math.min(topN, candidates.size());
      while (rank <= limit) {
        System.out.println("cand size: " + rank);
        double maxScore = -Double.MAX_VALUE;
        int bestIdx = 0;
        Hit bestCandidate = null;
        for (int i = 0; i < candidates.size(); i++) {
          if (selectedIdx[i] > 0) { // skip items already selected
            continue;
          }
          var cand = candidates.get(i);
          var correlation = computeAbsCorr(cand.query, cand.sketch);
          var maxAbsCorr = findMaxAbsCorr(cand, selected);
          var corrScore = Math.max(0.01, correlation - maxAbsCorr);
          var score = corrScore * cand.joinability();
          if (score > maxScore) {
            maxScore = score;
            bestIdx = i;
            bestCandidate = cand;
            cand.rerankScore = rank;
          }
        }
        selectedIdx[bestIdx] = rank++;
        selected.add(bestCandidate);
      }
      for (int i = 0; i < candidates.size(); i++) {
        if (selectedIdx[i] == 0) {
          var cand = candidates.get(i);
          cand.rerankScore = rank++;
          selected.add(cand);
        }
      }
      candidates.clear();
      candidates.addAll(selected);
    }
  }

  private static double findMaxAbsCorr(Hit hit, List<Hit> selected) {
    if (selected.isEmpty()) {
      return 0d;
    }
    double maxAbsCorr = -1;
    for (var i : selected) {
      double corrAbs = computeAbsCorr(i.sketch, hit.sketch);
      if (corrAbs > maxAbsCorr) {
        maxAbsCorr = corrAbs;
      }
    }
    return maxAbsCorr;
  }

  private static double computeAbsCorr(ImmutableCorrelationSketch a, ImmutableCorrelationSketch b) {
    double corrAbs = Math.abs(a.correlationTo(b).value);
    return Double.isNaN(corrAbs) ? 0.0 : corrAbs;
  }

  //    private static void recomputeScoreAndSort(List<Hit> hits, List<Hit> selected) {
  //      for (var hit : hits) {
  //        var correlation =  computeAbsCorr(hit.query, hit.sketch);
  //        var maxAbsCorr = findMaxAbsCorr(hit, selected);
  //        var corrScore = correlation - maxAbsCorr;
  //        hit.rerankScore = corrScore * hit.joinability();
  //      }
  //      hits.sort(RERANK_SCORE_DESC);
  //    }
  //
  //    public double[][] computeCorrelationMatrix(List<Hit> hits) {
  //      int size = hits.size();
  //      double[][] corrMatrix = new double[size][size];
  //      for (int i = 0; i < size; i++) {
  //        for (int j = i; j < size; j++) {
  //          var ski = hits.get(i).sketch;
  //          var skj = hits.get(j).sketch;
  //          double correlation = ski.correlationTo(skj).value;
  //          corrMatrix[i][j] = correlation;
  //          corrMatrix[j][i] = correlation;  // The matrix is symmetric
  //        }
  //      }
  //      return corrMatrix;
  //    }
}
