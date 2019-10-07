package sketches.correlation;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.Test;

public class CorrelationHashingTest {

  @Test
  public void test() {
    List<String> pk = Arrays.asList(new String[] {"a", "b", "c", "d", "e"});
    double[] q = new double[] {1.0, 2.0, 3.0, 4.0, 5.0};

    KMVCorrelationSketch qsk = new KMVCorrelationSketch(pk, q);

    double delta = 0.1;

    List<String> c4fk = Arrays.asList(new String[] {"a", "b", "c", "z", "x"});
    double[] c4 = new double[] {1.0, 2.0, 3.0, 4.0, 5.0};
    //        List<String> c4fk = Arrays.asList(new String[]{"a", "b", "c", "d"});
    //        double[] c4 = new double[]{1.0, 2.0, 3.0, 4.0};

    KMVCorrelationSketch c4sk = new KMVCorrelationSketch(c4fk, c4);
    System.out.println();
    System.out.println("         union: " + qsk.unionSize(c4sk));
    System.out.println("  intersection: " + qsk.intersectionSize(c4sk));
    System.out.println("       jaccard: " + qsk.jaccard(c4sk));
    System.out.println("cardinality(x): " + qsk.cardinality());
    System.out.println("cardinality(y): " + c4sk.cardinality());
    System.out.println("containment(x): " + qsk.containment(c4sk));
    System.out.println("containment(y): " + c4sk.containment(qsk));
    System.out.flush();
    System.err.flush();
    c4sk.setCardinality(5);
    qsk.setCardinality(5);
    System.out.println();
    System.out.println("         union: " + qsk.unionSize(c4sk));
    System.out.println("  intersection: " + qsk.intersectionSize(c4sk));
    System.out.println("       jaccard: " + qsk.jaccard(c4sk));
    System.out.println("cardinality(x): " + qsk.cardinality());
    System.out.println("cardinality(y): " + c4sk.cardinality());
    System.out.println("containment(x): " + qsk.containment(c4sk));
    System.out.println("containment(y): " + c4sk.containment(qsk));
  }

  @Test
  public void shouldEstimateCorrelationUsingKMVSketch() {
    List<String> pk = Arrays.asList(new String[] {"a", "b", "c", "d", "e"});
    double[] q = new double[] {1.0, 2.0, 3.0, 4.0, 5.0};

    List<String> fk = Arrays.asList(new String[] {"a", "b", "c", "d", "e"});
    double[] c0 = new double[] {1.0, 2.0, 3.0, 4.0, 5.0};
    double[] c1 = new double[] {1.1, 2.5, 3.0, 4.4, 5.9};
    double[] c2 = new double[] {1.0, 3.2, 3.1, 4.9, 5.4};

    KMVCorrelationSketch qsk = new KMVCorrelationSketch(pk, q);
    KMVCorrelationSketch c0sk = new KMVCorrelationSketch(fk, c0);
    KMVCorrelationSketch c1sk = new KMVCorrelationSketch(fk, c1);
    KMVCorrelationSketch c2sk = new KMVCorrelationSketch(fk, c2);

    double delta = 0.1;
    assertEquals(1.000, qsk.correlationTo(qsk), delta);
    assertEquals(1.000, qsk.correlationTo(c0sk), delta);
    assertEquals(0.9895, qsk.correlationTo(c1sk), delta);
    assertEquals(0.9558, qsk.correlationTo(c2sk), delta);
  }

  @Test
  public void shouldEstimateCorrelation() {
    List<String> pk = Arrays.asList(new String[] {"a", "b", "c", "d", "e"});
    double[] q1 = new double[] {1.0, 2.0, 3.0, 4.0, 5.0};

    List<String> fk = Arrays.asList(new String[] {"a", "b", "c", "d", "e"});
    double[] c0 = new double[] {1.0, 2.0, 3.0, 4.0, 5.0};
    double[] c1 = new double[] {1.1, 2.5, 3.0, 4.4, 5.9};
    double[] c2 = new double[] {1.0, 3.2, 3.1, 4.9, 5.4};

    MinhashCorrelationSketch q1sk = new MinhashCorrelationSketch(pk, q1);
    MinhashCorrelationSketch c0sk = new MinhashCorrelationSketch(fk, c0);
    MinhashCorrelationSketch c1sk = new MinhashCorrelationSketch(fk, c1);
    MinhashCorrelationSketch c2sk = new MinhashCorrelationSketch(fk, c2);

    double delta = 0.005;
    assertEquals(1.000, q1sk.correlationTo(q1sk), delta);
    assertEquals(1.000, q1sk.correlationTo(c0sk), delta);
    assertEquals(0.987, q1sk.correlationTo(c1sk), delta);
    assertEquals(0.947, q1sk.correlationTo(c2sk), delta);
  }
}
