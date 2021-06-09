package corrsketches.benchmark.index;

import static org.junit.jupiter.api.Assertions.assertEquals;

import corrsketches.benchmark.ColumnPair;
import corrsketches.benchmark.index.SketchIndex.Hit;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.Test;

public class SketchIndexTest {

  @Test
  public void shouldIndexSketches() throws IOException {
    ColumnPair q =
        createColumnPair(
            Arrays.asList("a", "b", "c", "d", "e"), new double[] {1.0, 2.0, 3.0, 4.0, 5.0});

    ColumnPair c0 =
        createColumnPair(
            Arrays.asList("a", "b", "c", "d", "e"), new double[] {1.0, 2.0, 3.0, 4.0, 5.0});

    ColumnPair c1 =
        createColumnPair(Arrays.asList("a", "b", "c", "d"), new double[] {1.1, 2.5, 3.0, 4.4});

    ColumnPair c2 = createColumnPair(Arrays.asList("a", "b", "c"), new double[] {1.0, 3.1, 3.2});

    SketchIndex index = new SketchIndex();
    index.index("c0", c0);
    index.index("c1", c1);
    index.index("c2", c2);

    List<Hit> hits = index.search(q, 5);

    System.out.println("Total hits: " + hits.size());
    for (int i = 0; i < hits.size(); i++) {
      Hit hit = hits.get(i);
      System.out.printf("\n[%d] ", i + 1);
      System.out.println("id: " + hit.id);
      System.out.println("    score: " + hit.score);
      //      System.out.println("    containment: " + hit.containment());
      System.out.println("    correlation: " + hit.correlation());
      System.out.println("    robust-correlation: " + hit.robustCorrelation());
    }

    assertEquals(hits.size(), 3);
    assertEquals(hits.get(0).id, "c0");
    assertEquals(hits.get(1).id, "c1");
    assertEquals(hits.get(2).id, "c2");
  }

  @Test
  public void shouldIndexSketchesWithQCRIndex() throws IOException {
    ColumnPair q =
        createColumnPair(
            Arrays.asList("a", "b", "c", "d", "e", "f", "g", "h"),
            new double[] {1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0});

    ColumnPair c0 =
        createColumnPair(
            Arrays.asList("a", "b", "c", "d", "e", "f", "g"), new double[] {1, 2, 3, 4, 5, 6, 7});

    ColumnPair c1 =
        createColumnPair(
            Arrays.asList("a", "b", "c", "d", "e", "f", "g"), new double[] {7, 6, 5, 4, 3, 2, 1});

    ColumnPair c2 =
        createColumnPair(
            Arrays.asList("a", "b", "c", "d", "e"), new double[] {1.1, 2.5, 3.0, 4.4, 4.6});

    ColumnPair c3 =
        createColumnPair(
            Arrays.asList("a", "b", "c", "d", "e"), new double[] {1.1, 2.5, 2.0, 2.7, 3.0});

    ColumnPair c4 =
        createColumnPair(
            Arrays.asList("a", "b", "c", "d", "e"), new double[] {1.5, 1.5, 1.0, 1.0, 2.0});

    ColumnPair c5 =
        createColumnPair(
            Arrays.asList("a", "b", "c", "d", "e"), new double[] {5.0, 4.1, 3.1, 2.0, 1.0});

    // ColumnPair c5 =
    //     createColumnPair(Arrays.asList("a", "b", "c", "d", "e", "f", "g", "h"), new double[] {8,
    // 7, 6, 5, 4, 3, 2, 1});

    QCRSketchIndex index = new QCRSketchIndex();
    index.index("c0", c0);
    index.index("c1", c1);
    index.index("c2", c2);
    index.index("c3", c3);
    index.index("c4", c4);
    index.index("c5", c5);

    List<Hit> hits = index.search(q, 6);

    System.out.println("Total hits: " + hits.size());
    for (int i = 0; i < hits.size(); i++) {
      Hit hit = hits.get(i);
      System.out.printf("\n[%d] ", i + 1);
      System.out.println("id: " + hit.id);
      System.out.printf("          score:  %.3f\n", hit.score);
      //      System.out.println("    containment: " + hit.containment());
      System.out.printf("    correlation: %+.3f\n", hit.correlation());
      System.out.printf("            qcr: %+.3f\n", hit.qcrCorrelation());
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

  public ColumnPair createColumnPair(List<String> keyValues, double[] columnValues) {
    ColumnPair cp = new ColumnPair();
    cp.columnValues = columnValues;
    cp.keyValues = keyValues;
    return cp;
  }
}
