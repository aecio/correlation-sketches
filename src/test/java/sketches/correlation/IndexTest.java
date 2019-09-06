package sketches.correlation;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.Test;

public class IndexTest {

    @Test
    public void shouldIndexSketches() throws IOException {
        List<String> pk = Arrays.asList(new String[]{"a", "b", "c", "d", "e"});
        double[] q = new double[]{1.0, 2.0, 3.0, 4.0, 5.0};

        List<String> c0fk = Arrays.asList(new String[]{"a", "b", "c", "d", "e"});
        double[] c0 = new double[]{1.0, 2.0, 3.0, 4.0, 5.0};

        List<String> c1fk = Arrays.asList(new String[]{"a", "b", "c", "d"});
        double[] c1 = new double[]{1.1, 2.5, 3.0, 4.4};

        List<String> c2fk = Arrays.asList(new String[]{"a", "b", "c"});
        double[] c2 = new double[]{1.0, 3.2, 3.1};

        KMVCorrelationSketch qsk = new KMVCorrelationSketch(pk, q);
        KMVCorrelationSketch c0sk = new KMVCorrelationSketch(c0fk, c0);
        KMVCorrelationSketch c1sk = new KMVCorrelationSketch(c1fk, c1);
        KMVCorrelationSketch c2sk = new KMVCorrelationSketch(c2fk, c2);

        SketchIndex index = new SketchIndex();
        index.index("c0", c0sk);
        index.index("c1", c1sk);
        index.index("c2", c2sk);

        List<KMVCorrelationSketch> hits = index.search(qsk, 5);
        System.out.println(hits.get(0));
        System.out.println(hits.get(1));
        System.out.println(hits.get(2));

        double delta = 0.1;
        assertEquals(1.000, qsk.correlationTo(qsk), delta);
        assertEquals(1.000, qsk.correlationTo(c0sk), delta);
        assertEquals(0.985, qsk.correlationTo(c1sk), delta);
        assertEquals(0.845, qsk.correlationTo(c2sk), delta);
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
