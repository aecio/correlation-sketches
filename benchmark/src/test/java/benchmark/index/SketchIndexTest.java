package benchmark.index;

import static org.junit.jupiter.api.Assertions.assertEquals;

import benchmark.ColumnPair;
import benchmark.index.SketchIndex.Hit;
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
