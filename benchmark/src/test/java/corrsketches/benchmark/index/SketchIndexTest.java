package corrsketches.benchmark.index;

import static org.junit.jupiter.api.Assertions.assertEquals;

import corrsketches.ColumnType;
import corrsketches.CorrelationSketch;
import corrsketches.benchmark.ColumnPair;
import corrsketches.benchmark.IndexCorrelationBenchmark;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.Test;

public class SketchIndexTest {

  @Test
  public void shouldIndexSketches() throws IOException {
    ColumnPair q =
        createNumericalColumnPair(
            Arrays.asList("a", "b", "c", "d", "e"), new double[] {1.0, 2.0, 3.0, 4.0, 5.0});

    ColumnPair c0 =
        createNumericalColumnPair(
            Arrays.asList("a", "b", "c", "d", "e"), new double[] {1.0, 2.0, 3.0, 4.0, 5.0});

    ColumnPair c1 =
        createNumericalColumnPair(
            Arrays.asList("a", "b", "c", "d"), new double[] {1.1, 2.5, 3.0, 4.4});

    ColumnPair c2 =
        createNumericalColumnPair(Arrays.asList("a", "b", "c"), new double[] {1.0, 3.1, 3.2});

    SketchIndex index = new SketchIndex();
    index.index("c0", c0);
    index.index("c1", c1);
    index.index("c2", c2);
    index.refresh();

    List<Hit> hits = index.search(q, 5);

    System.out.println("Total hits: " + hits.size());
    for (int i = 0; i < hits.size(); i++) {
      Hit hit = hits.get(i);
      System.out.printf("\n[%d] ", i + 1);
      System.out.println("id: " + hit.id);
      System.out.println("    score: " + hit.score);
      System.out.println("    correlation: " + hit.correlation());
    }

    assertEquals(3, hits.size());
    assertEquals("c0", hits.get(0).id);
    assertEquals("c1", hits.get(1).id);
    assertEquals("c2", hits.get(2).id);
  }

  @Test
  public void shouldIndexSketchesWithQCRISketchIndex() throws IOException {
    QCRISketchIndex index = new QCRISketchIndex();
    shouldRetrieveCorrelatedColumns(index);
  }

  @Test
  public void shouldIndexSketchesWithQCRSketchIndex() throws IOException {
    QCRSketchIndex index = new QCRSketchIndex();
    shouldRetrieveCorrelatedColumns(index);
  }

  @Test
  public void shouldIndexSketchesWithQCRSketchIndexSortedByKey() throws IOException {
    QCRSketchIndex index =
        new QCRSketchIndex(
            null, new CorrelationSketch.Builder(), IndexCorrelationBenchmark.SortBy.KEY, false);
    shouldRetrieveCorrelatedColumns(index);
  }

  private void shouldRetrieveCorrelatedColumns(SketchIndex index) throws IOException {
    ColumnPair q =
        createNumericalColumnPair(
            Arrays.asList("a", "b", "c", "d", "e", "f", "g", "h"),
            new double[] {1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0});

    ColumnPair c0 =
        createNumericalColumnPair(
            Arrays.asList("a", "b", "c", "d", "e", "f", "g"), new double[] {1, 2, 3, 4, 5, 6, 7});

    ColumnPair c1 =
        createNumericalColumnPair(
            Arrays.asList("a", "b", "c", "d", "e", "f", "g"), new double[] {7, 6, 5, 4, 3, 2, 1});

    ColumnPair c2 =
        createNumericalColumnPair(
            Arrays.asList("a", "b", "c", "d", "e"), new double[] {1.1, 2.5, 3.0, 4.4, 4.6});

    ColumnPair c3 =
        createNumericalColumnPair(
            Arrays.asList("a", "b", "c", "d", "e"), new double[] {1.1, 2.5, 2.0, 2.7, 3.0});

    ColumnPair c4 =
        createNumericalColumnPair(
            Arrays.asList("a", "b", "c", "d", "e"), new double[] {1.5, 1.5, 1.0, 1.0, 2.0});

    ColumnPair c5 =
        createNumericalColumnPair(
            Arrays.asList("a", "b", "c", "d", "e"), new double[] {5.0, 4.1, 3.1, 2.0, 1.0});

    ColumnPair c6 =
        createNumericalColumnPair(
            Arrays.asList("a", "b", "c", "d", "e"), new double[] {1.0, 1.0, 1.0, 1.0, 1.0});

    index.index("c0", c0);
    index.index("c1", c1);
    index.index("c2", c2);
    index.index("c3", c3);
    index.index("c4", c4);
    index.index("c5", c5);
    index.index("c6", c6);
    index.refresh();

    List<Hit> hits = index.search(q, 6);

    System.out.println("Total hits: " + hits.size());
    for (int i = 0; i < hits.size(); i++) {
      Hit hit = hits.get(i);
      System.out.printf("\n[%d] ", i + 1);
      System.out.printf("     id: %s\n", hit.id);
      System.out.printf("      score: %.3f\n", hit.score);
      System.out.printf("correlation: %.3f\n", hit.correlation());
    }

    assertEquals(6, hits.size());
    assertEquals("c0", hits.get(0).id);
    assertEquals("c1", hits.get(1).id);
    assertEquals("c2", hits.get(2).id);
  }

  @Test
  public void shouldEncodeAndDecodeDoubleArrayToBytes() {
    double[] doubles = new double[] {1.1, 2.2, 3.3};
    byte[] bytes = SketchIndex.toByteArray(doubles);
    double[] decoded = SketchIndex.toDoubleArray(bytes);
    for (int i = 0; i < decoded.length; i++) {
      assertEquals(decoded[i], doubles[i], 0.001);
    }
  }

  public ColumnPair createNumericalColumnPair(List<String> keyValues, double[] columnValues) {
    ColumnPair cp = new ColumnPair();
    cp.columnValues = columnValues;
    cp.keyValues = keyValues;
    cp.columnValueType = ColumnType.NUMERICAL;
    return cp;
  }
}
