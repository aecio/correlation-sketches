package sketches.correlation;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Arrays;
import java.util.List;
import java.util.Random;
import org.junit.jupiter.api.Test;
import sketches.correlation.Correlation.Estimate;
import sketches.correlation.KMVCorrelationSketch.ImmutableCorrelationSketch;
import sketches.correlation.KMVCorrelationSketch.ImmutableCorrelationSketch.Paired;
import sketches.kmv.KMV;
import sketches.util.RandomArrays;
import sketches.util.RandomArrays.CI;

public class CorrelationSketchTest {

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
    assertEquals(1.000, qsk.correlationTo(qsk).coefficient, delta);
    assertEquals(1.000, qsk.correlationTo(c0sk).coefficient, delta);
    assertEquals(0.9895, qsk.correlationTo(c1sk).coefficient, delta);
    assertEquals(0.9558, qsk.correlationTo(c2sk).coefficient, delta);
  }

  @Test
  public void shouldCreateImmutableCorrelationSketch() {
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

    ImmutableCorrelationSketch iqsk = qsk.toImmutable();
    ImmutableCorrelationSketch ic0sk = c0sk.toImmutable();
    ImmutableCorrelationSketch ic1sk = c1sk.toImmutable();
    ImmutableCorrelationSketch ic2sk = c2sk.toImmutable();

    double delta = 0.1;
    assertEquals(1.000, qsk.correlationTo(qsk).coefficient, delta);
    assertEquals(1.000, iqsk.correlationTo(iqsk).coefficient, delta);

    assertEquals(1.000, qsk.correlationTo(c0sk).coefficient, delta);
    assertEquals(1.000, iqsk.correlationTo(ic0sk).coefficient, delta);

    assertEquals(0.9895, qsk.correlationTo(c1sk).coefficient, delta);
    assertEquals(0.9895, iqsk.correlationTo(ic1sk).coefficient, delta);

    assertEquals(0.9558, qsk.correlationTo(c2sk).coefficient, delta);
    assertEquals(0.9558, iqsk.correlationTo(ic2sk).coefficient, delta);
  }

  @Test
  public void shouldCreateImmutableSketch() {
    List<String> xkeys = Arrays.asList(new String[] {"a", "b", "c", "d", "f"});
    double[] xvalues = new double[] {1.0, 2.0, 3.0, 4.0, 5.0};

    List<String> ykeys = Arrays.asList(new String[] {"!", "a", "b", "c", "d", "e"});
    double[] yvalues = new double[] {0.0, 2.0, 3.0, 4.0, 5.0, 6.0};

    KMVCorrelationSketch xs = new KMVCorrelationSketch(xkeys, xvalues, 5);
    KMVCorrelationSketch ys = new KMVCorrelationSketch(ykeys, yvalues, 5);

    final ImmutableCorrelationSketch xsi = xs.toImmutable();
    final ImmutableCorrelationSketch ysi = ys.toImmutable();

    final Estimate estimate = xs.correlationTo(ys);
    final Estimate estimateImmutable = xsi.correlationTo(ysi);
    final Paired intersection = xsi.intersection(ysi);

    System.out.println(intersection);
    assertEquals(3, estimate.sampleSize);
    assertEquals(intersection.keys.length, estimate.sampleSize);
    assertEquals(estimate.sampleSize, estimateImmutable.sampleSize);
    assertEquals(estimate.coefficient, estimateImmutable.coefficient);
    assertEquals(1.0, estimateImmutable.coefficient);
  }

  @Test
  public void shouldComputeCorrelationUsingImmutableSketchOnRamdomVectors() {
    Random r = new Random();

    int runs = 5000;
    long[] runningTimes = new long[runs];
    long[] runningTimesImmutable = new long[runs];

    for (int i = 0; i < runs; i++) {
      final double jc = r.nextDouble();
      //      final int n = (25 + r.nextInt(512)) * 1000;
      final int n = 10_000;
      double[] y = new double[n];
      double[] x = new double[n];
      String[] kx = new String[n];
      String[] ky = new String[n];
      for (int j = 0; j < n; j++) {
        x[j] = r.nextGaussian() * (1_000_000);
        //      y[j] = r.nextGaussian();
        //      y[j] = x[j] + (r.nextGaussian() > r.nextGaussian() ? 3 :
        // Math.log(1-r.nextDouble())/-1); // r.nextGaussian());
        //      y[j] = x[j]*(-10)*r.nextGaussian();
        y[j] = Math.log(1 - r.nextDouble()) / (-1);

        if (r.nextDouble() < jc) {
          String k = String.valueOf(r.nextInt());
          kx[j] = k;
          ky[j] = k;
        } else {
          kx[j] = String.valueOf(r.nextInt());
          ky[j] = String.valueOf(r.nextInt());
        }
      }

      int k = 256;

      final KMV xkmv = new KMV(k);
      xkmv.updateAll(Arrays.asList(kx), x);
      KMVCorrelationSketch xsketch = new KMVCorrelationSketch(xkmv);

      final KMV ykmv = new KMV(k);
      ykmv.updateAll(Arrays.asList(ky), y);
      KMVCorrelationSketch ysketch = new KMVCorrelationSketch(ykmv);

      final Estimate estimate1;
      final Estimate estimate2;
      long t0;
      long t1;
      if (i % 2 == 0) {
        t0 = System.nanoTime();
        estimate1 = xsketch.toImmutable().correlationTo(ysketch.toImmutable());
        t1 = System.nanoTime();
        runningTimesImmutable[i] = t1 - t0;

        t0 = System.nanoTime();
        estimate2 = xsketch.correlationTo(ysketch);
        t1 = System.nanoTime();
        runningTimes[i] = t1 - t0;
      } else {
        t0 = System.nanoTime();
        estimate2 = xsketch.correlationTo(ysketch);
        t1 = System.nanoTime();
        runningTimes[i] = t1 - t0;

        t0 = System.nanoTime();
        estimate1 = xsketch.toImmutable().correlationTo(ysketch.toImmutable());
        t1 = System.nanoTime();
        runningTimesImmutable[i] = t1 - t0;
      }
      assertEquals(estimate2.coefficient, estimate1.coefficient);
    }

    double alpha = 0.05;

    CI ci = RandomArrays.percentiles(runningTimes, alpha);
    System.out.printf("mean time: %.3f ms\n", ci.mean / 1_000_000.);
    System.out.printf("lb: %.3f\n", ci.lb / 1_000_000.);
    System.out.printf("ub: %.3f\n", ci.ub / 1_000_000.);

    CI ci2 = RandomArrays.percentiles(runningTimesImmutable, alpha);
    System.out.printf("mean time: %.3f ms\n", ci2.mean / 1_000_000.);
    System.out.printf("lb: %.3f\n", ci2.lb / 1_000_000.);
    System.out.printf("ub: %.3f\n", ci2.ub / 1_000_000.);
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
