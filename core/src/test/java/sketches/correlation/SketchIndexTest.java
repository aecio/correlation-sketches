package sketches.correlation;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.Test;
import sketches.correlation.SketchIndex.Hit;

public class SketchIndexTest {

  @Test
  public void shouldIndexSketches() throws IOException {
    List<String> pk = Arrays.asList(new String[] {"a", "b", "c", "d", "e"});
    double[] q = new double[] {1.0, 2.0, 3.0, 4.0, 5.0};

    List<String> c0fk = Arrays.asList(new String[] {"a", "b", "c", "d", "e"});
    double[] c0 = new double[] {1.0, 2.0, 3.0, 4.0, 5.0};

    List<String> c1fk = Arrays.asList(new String[] {"a", "b", "c", "d"});
    double[] c1 = new double[] {1.1, 2.5, 3.0, 4.4};

    List<String> c2fk = Arrays.asList(new String[] {"a", "b", "c"});
    double[] c2 = new double[] {1.0, 3.2, 3.1};

    KMVCorrelationSketch qsk = new KMVCorrelationSketch(pk, q);
    KMVCorrelationSketch c0sk = new KMVCorrelationSketch(c0fk, c0);
    KMVCorrelationSketch c1sk = new KMVCorrelationSketch(c1fk, c1);
    KMVCorrelationSketch c2sk = new KMVCorrelationSketch(c2fk, c2);

    SketchIndex index = new SketchIndex();
    index.index("c0", c0sk);
    index.index("c1", c1sk);
    index.index("c2", c2sk);

    List<Hit> hits = index.search(qsk, 5);

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
}
