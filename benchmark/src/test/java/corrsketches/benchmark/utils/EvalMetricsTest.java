package corrsketches.benchmark.utils;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.google.common.collect.ImmutableMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.junit.jupiter.api.Test;

public class EvalMetricsTest {

  @Test
  public void shouldComputeNDCG() {
    // Numerical examples taken from the scikit-learn documentations

    Map<String, Double> relevanceMap =
        ImmutableMap.of(
            "1", 10d,
            "2", 0d,
            "3", 0d,
            "4", 1d,
            "5", 5d);

    EvalMetrics metrics = new EvalMetrics(relevanceMap);

    double dcg = metrics.dgcIds(List.of("5", "4", "3", "2", "1"), 5);
    assertEquals(9.49, dcg, 0.01);

    double ndcg = metrics.ndgcIds(List.of("5", "4", "3", "2", "1"), 5);
    assertEquals(0.69, ndcg, 0.01);

    double dcgAt2 = metrics.dgcIds(List.of("5", "4", "3", "2", "1"), 2);
    assertEquals(5.63, dcgAt2, 0.01);

    double dcgAt1 = metrics.dgcIds(List.of("5", "1"), 1);
    assertEquals(5, dcgAt1, 0.01);

    ndcg = metrics.ndgcIds(List.of("2", "3", "4", "1", "5"), 5);
    assertEquals(0.49, ndcg, 0.01);

    ndcg = metrics.ndgcIds(List.of("2", "3", "4", "1", "5"), 4);
    assertEquals(0.35, ndcg, 0.01);

    ndcg = metrics.ndgcIds(List.of("1", "5", "4", "3", "2"), 4);
    assertEquals(1.0, ndcg, 0.01);
    ndcg = metrics.ndgcIds(List.of("1", "5", "4", "3", "2"), 5);
    assertEquals(1.0, ndcg, 0.01);
    ndcg = metrics.ndgcIds(List.of("1", "5", "4"), 3);
    assertEquals(1.0, ndcg, 0.01);
    ndcg = metrics.ndgcIds(List.of("1", "5", "4"), 2);
    assertEquals(1.0, ndcg, 0.01);
    ndcg = metrics.ndgcIds(List.of("1", "5", "4"), 10); // out of bounds
    assertEquals(1.0, ndcg, 0.01);
  }

  @Test
  public void shouldComputeDCG() {
    Map<String, Double> relevanceMap = new TreeMap<>();
    relevanceMap.put("1", 3d);
    relevanceMap.put("2", 2d);
    relevanceMap.put("3", 3d);
    relevanceMap.put("4", 0d);
    relevanceMap.put("5", 1d);
    relevanceMap.put("6", 2d);

    EvalMetrics metrics = new EvalMetrics(relevanceMap);

    final List<String> rank = List.of("1", "2", "3", "4", "5", "6");
    double dcg = metrics.dgcIds(rank, rank.size());
    double ndcg = metrics.ndgcIds(rank, rank.size());

    assertEquals(6.861, dcg, 0.001);
    assertEquals(0.961, ndcg, 0.001);
  }

  @Test
  public void shouldComputeNDCGTreatingNaNsAsZero() {
    Map<String, Double> relevanceMap = new TreeMap<>();
    relevanceMap.put("1", 3d);
    relevanceMap.put("2", 2d);
    relevanceMap.put("3", 3d);
    relevanceMap.put("4", Double.NaN);
    relevanceMap.put("5", 1d);
    relevanceMap.put("6", 2d);

    EvalMetrics metrics = new EvalMetrics(relevanceMap);

    List<String> rank = List.of("1", "2", "3", "4", "5", "6");
    double dcg = metrics.dgcIds(rank, rank.size());
    double ndcg = metrics.ndgcIds(rank, rank.size());

    assertEquals(6.861, dcg, 0.001);
    assertEquals(0.961, ndcg, 0.001);

    rank = List.of("1", "2", "3", "4", "5", "6", "7"); // id not in relevance map
    dcg = metrics.dgcIds(rank, rank.size());
    ndcg = metrics.ndgcIds(rank, rank.size());

    assertEquals(6.861, dcg, 0.001);
    assertEquals(0.961, ndcg, 0.001);
  }

  @Test
  public void shouldComputeRecall() {
    Map<String, Double> relevanceMap = new TreeMap<>();
    relevanceMap.put("1", 0.20d);
    relevanceMap.put("2", 0.10);
    relevanceMap.put("3", 0.40d);
    relevanceMap.put("4", 0.90d);
    relevanceMap.put("5", 0.01d);
    relevanceMap.put("6", 1.00d);

    EvalMetrics metrics = new EvalMetrics(relevanceMap);

    double recall;
    final List<String> rank = List.of("1", "2", "3");

    recall = metrics.recallIds(rank, 0.2);
    assertEquals(2 / 4d, recall, 0.001);

    recall = metrics.recallIds(rank, 0.2);
    assertEquals(2 / 4d, recall, 0.001);

    recall = metrics.recallIds(rank, 0.5);
    assertEquals(0 / 4d, recall, 0.001);

    recall = metrics.recallIds(rank, 0.2);
    assertEquals(2 / 4d, recall, 0.001);

    recall = metrics.recallIds(rank, 0.75);
    assertEquals(0 / 2d, recall, 0.001);

    recall = metrics.recallIds(rank, 0.75);
    assertEquals(0 / 2d, recall, 0.001);
  }
}
