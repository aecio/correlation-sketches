package corrsketches.benchmark.utils;

import corrsketches.benchmark.index.Hit;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class EvalMetrics {

  private final double[] idealRel;
  private final Map<String, Double> relevanceMap;

  public EvalMetrics(Map<String, Double> relevanceMap) {
    this.relevanceMap = relevanceMap;
    this.idealRel =
        relevanceMap.values().stream()
            .map(EvalMetrics::ensureNonNullOrNaN)
            .sorted(Comparator.reverseOrder())
            .mapToDouble(i -> i)
            .toArray();
  }

  public double ndgc(List<Hit> hits, int k) {
    return ndgcIds(mapToIds(hits), k);
  }

  double ndgcIds(List<String> hitIds, int k) {
    double[] rel = mapIdsToGradedRelevance(hitIds);
    return dgc(rel, k) / dgc(idealRel, k);
  }

  double dgcIds(List<String> hitIds, int k) {
    double[] rel = mapIdsToGradedRelevance(hitIds);
    return dgc(rel, k);
  }

  private static double dgc(double[] rel, int k) {
    double dcg = 0d;
    for (int i = 0; i < Math.min(k, rel.length); i++) {
      // final double num = Math.pow(2, rel[i]);
      final double num = rel[i];
      final double den = log2(i + 2);
      dcg += num / den;
    }
    return dcg;
  }

  public double recall(List<Hit> hits, double threshold) {
    return recallIds(mapToIds(hits), threshold);
  }

  double recallIds(List<String> hitIds, double threshold) {
    int[] binRel25 = binaryRelevance(hitIds, threshold);
    int retrieved = 0;
    for (int r : binRel25) {
      retrieved += r;
    }
    int totalRelevant = relevanceMap.values().stream().mapToInt(i -> i >= threshold ? 1 : 0).sum();
    if (totalRelevant == 0) {
      return Double.NaN;
    }
    return retrieved / (double) totalRelevant;
  }

  private List<String> mapToIds(List<Hit> hits) {
    return hits.stream().map(h -> h.id).collect(Collectors.toList());
  }

  public double[] mapHitsToGradedRelevance(List<Hit> hits) {
    return mapIdsToGradedRelevance(mapToIds(hits));
  }

  public double[] mapIdsToGradedRelevance(List<String> hitIds) {
    double[] rel = new double[hitIds.size()];
    for (int i = 0; i < hitIds.size(); i++) {
      rel[i] = ensureNonNullOrNaN(relevanceMap.get(hitIds.get(i)));
    }
    return rel;
  }

  private static Double ensureNonNullOrNaN(Double rel) {
    if (rel == null || rel.isNaN()) {
      return 0.0;
    }
    return rel;
  }

  private static double log2(final double value) {
    return Math.log(value) / Math.log(2);
  }

  private int[] binaryRelevance(List<String> hitIds, double threshold) {
    int[] rel = new int[hitIds.size()];
    for (int i = 0; i < hitIds.size(); i++) {
      rel[i] = relevanceMap.get(hitIds.get(i)) >= threshold ? 1 : 0;
    }
    return rel;
  }
}
