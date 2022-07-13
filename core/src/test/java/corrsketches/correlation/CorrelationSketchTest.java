package corrsketches.correlation;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.byLessThan;
import static org.junit.jupiter.api.Assertions.assertEquals;

import corrsketches.CorrelationSketch;
import corrsketches.CorrelationSketch.Builder;
import corrsketches.CorrelationSketch.ImmutableCorrelationSketch;
import corrsketches.CorrelationSketch.ImmutableCorrelationSketch.Join;
import corrsketches.MinhashCorrelationSketch;
import corrsketches.SketchType;
import corrsketches.aggregations.AggregateFunction;
import corrsketches.util.RandomArrays;
import corrsketches.util.RandomArrays.CI;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import org.junit.jupiter.api.Test;

public class CorrelationSketchTest {

  @Test
  public void test() {
    List<String> pk = Arrays.asList("a", "b", "c", "d", "e");
    double[] q = new double[] {1.0, 2.0, 3.0, 4.0, 5.0};

    final Builder builder = CorrelationSketch.builder();

    CorrelationSketch qsk = builder.build(pk, q);

    List<String> c4fk = Arrays.asList("a", "b", "c", "z", "x");
    double[] c4 = new double[] {1.0, 2.0, 3.0, 4.0, 5.0};
    //        List<String> c4fk = Arrays.asList(new String[]{"a", "b", "c", "d"});
    //        double[] c4 = new double[]{1.0, 2.0, 3.0, 4.0};

    CorrelationSketch c4sk = builder.build(c4fk, c4);
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
    List<String> pk = Arrays.asList("a", "b", "c", "d", "e");
    double[] q = new double[] {1.0, 2.0, 3.0, 4.0, 5.0};

    List<String> fk = Arrays.asList("a", "b", "c", "d", "e");
    double[] c0 = new double[] {1.0, 2.0, 3.0, 4.0, 5.0};
    double[] c1 = new double[] {1.1, 2.5, 3.0, 4.4, 5.9};
    double[] c2 = new double[] {1.0, 3.2, 3.1, 4.9, 5.4};

    final Builder builder = CorrelationSketch.builder();

    CorrelationSketch qsk = builder.build(pk, q);
    CorrelationSketch c0sk = builder.build(fk, c0);
    CorrelationSketch c1sk = builder.build(fk, c1);
    CorrelationSketch c2sk = builder.build(fk, c2);

    double delta = 0.1;
    assertEquals(1.000, qsk.correlationTo(qsk).value, delta);
    assertEquals(1.000, qsk.correlationTo(c0sk).value, delta);
    assertEquals(0.9895, qsk.correlationTo(c1sk).value, delta);
    assertEquals(0.9558, qsk.correlationTo(c2sk).value, delta);
  }

  @Test
  public void shouldEstimateCorrelationBetweenColumnAggregations() {
    List<String> kx = Arrays.asList("a", "a", "b", "b", "c", "d");
    // sum: a=1 b=2 c=3 d=4, mean: a=0.5 b=1 c=3 d=4, count: a=2, c=2, c=1, d=1
    double[] x = new double[] {-20., 21.0, 1.0, 1.0, 3.0, 4.0};

    List<String> ky = Arrays.asList("a", "b", "c", "d");
    double[] ysum = new double[] {1.0, 2.0, 3.0, 4.0};
    double[] ymean = new double[] {0.5, 1.0, 3.0, 4.0};
    double[] ycount = new double[] {2.0, 2.0, 1.0, 1.0};

    final Builder builder = CorrelationSketch.builder().aggregateFunction(AggregateFunction.FIRST);

    CorrelationSketch csySum = builder.build(ky, ysum);
    CorrelationSketch csyMean = builder.build(ky, ymean);
    CorrelationSketch csyCount = builder.build(ky, ycount);

    CorrelationSketch csxSum = builder.aggregateFunction(AggregateFunction.SUM).build(kx, x);
    CorrelationSketch csxMean = builder.aggregateFunction(AggregateFunction.MEAN).build(kx, x);
    CorrelationSketch csxCount = builder.aggregateFunction(AggregateFunction.COUNT).build(kx, x);

    double delta = 0.0001;
    assertEquals(1.000, csxSum.correlationTo(csySum).value, delta);
    assertEquals(1.000, csxMean.correlationTo(csyMean).value, delta);
    assertEquals(1.000, csxCount.correlationTo(csyCount).value, delta);
  }

  @Test
  public void shouldCreateImmutableCorrelationSketch() {
    List<String> pk = Arrays.asList("a", "b", "c", "d", "e");
    double[] q = new double[] {1.0, 2.0, 3.0, 4.0, 5.0};

    List<String> fk = Arrays.asList("a", "b", "c", "d", "e");
    double[] c0 = new double[] {1.0, 2.0, 3.0, 4.0, 5.0};
    double[] c1 = new double[] {1.1, 2.5, 3.0, 4.4, 5.9};
    double[] c2 = new double[] {1.0, 3.2, 3.1, 4.9, 5.4};

    final Builder builder = CorrelationSketch.builder();
    CorrelationSketch qsk = builder.build(pk, q);
    CorrelationSketch c0sk = builder.build(fk, c0);
    CorrelationSketch c1sk = builder.build(fk, c1);
    CorrelationSketch c2sk = builder.build(fk, c2);

    ImmutableCorrelationSketch iqsk = qsk.toImmutable();
    ImmutableCorrelationSketch ic0sk = c0sk.toImmutable();
    ImmutableCorrelationSketch ic1sk = c1sk.toImmutable();
    ImmutableCorrelationSketch ic2sk = c2sk.toImmutable();

    double delta = 0.1;
    assertEquals(1.000, qsk.correlationTo(qsk).value, delta);
    assertEquals(1.000, iqsk.correlationTo(iqsk).value, delta);

    assertEquals(1.000, qsk.correlationTo(c0sk).value, delta);
    assertEquals(1.000, iqsk.correlationTo(ic0sk).value, delta);

    assertEquals(0.9895, qsk.correlationTo(c1sk).value, delta);
    assertEquals(0.9895, iqsk.correlationTo(ic1sk).value, delta);

    assertEquals(0.9558, qsk.correlationTo(c2sk).value, delta);
    assertEquals(0.9558, iqsk.correlationTo(ic2sk).value, delta);
  }

  @Test
  public void shouldCreateImmutableSketch() {
    List<String> xkeys = Arrays.asList("a", "b", "c", "d", "f");
    double[] xvalues = new double[] {1.0, 2.0, 3.0, 4.0, 5.0};

    List<String> ykeys = Arrays.asList("!", "a", "b", "c", "d", "e");
    double[] yvalues = new double[] {0.0, 2.0, 3.0, 4.0, 5.0, 6.0};

    final Builder builder = CorrelationSketch.builder().sketchType(SketchType.KMV, 5);

    CorrelationSketch xs = builder.build(xkeys, xvalues);
    CorrelationSketch ys = builder.build(ykeys, yvalues);

    final ImmutableCorrelationSketch xsi = xs.toImmutable();
    final ImmutableCorrelationSketch ysi = ys.toImmutable();

    final Estimate estimate = xs.correlationTo(ys);
    final Estimate estimateImmutable = xsi.correlationTo(ysi);
    final Join intersection = xsi.join(ysi);

    System.out.println(intersection);
    assertEquals(3, estimate.sampleSize);
    assertEquals(intersection.keys.length, estimate.sampleSize);
    assertEquals(estimate.sampleSize, estimateImmutable.sampleSize);
    assertEquals(estimate.value, estimateImmutable.value);
    assertEquals(1.0, estimateImmutable.value);
  }

  @Test
  public void shouldComputeCorrelationUsingImmutableSketchOnRandomVectors() {
    Random r = new Random();

    int runs = 100;
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
      Builder builder = CorrelationSketch.builder().sketchType(SketchType.KMV, k);

      CorrelationSketch xsketch = builder.build(kx, x);
      CorrelationSketch ysketch = builder.build(ky, y);

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
      assertEquals(estimate2.value, estimate1.value);
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
    List<String> pk = Arrays.asList("a", "b", "c", "d", "e");
    double[] q1 = new double[] {1.0, 2.0, 3.0, 4.0, 5.0};

    List<String> fk = Arrays.asList("a", "b", "c", "d", "e");
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

  @Test
  public void shouldEstimateMutualInformation() {
    List<String> pk = Arrays.asList("a", "b", "c", "d", "e", "f", "g", "h", "i", "j");
    double[] q = new double[] {1, 1, 1, 2, 2, 2, 2, 2, 3, 3};

    List<String> fk = Arrays.asList("a", "b", "c", "d", "e", "f", "g", "h", "i", "j");
    double[] c0 = new double[] {1, 1, 1, 2, 2, 2, 2, 2, 3, 3};
    double[] c1 = new double[] {1, 2, 2, 3, 2, 3, 2, 3, 1, 2};
    double[] c2 = new double[] {1, 2, 2, 1, 2, 3, 2, 3, 2, 2};

    final Builder builder =
        CorrelationSketch.builder().estimator(CorrelationType.MUTUAL_INFORMATION);

    CorrelationSketch qsk = builder.build(pk, q);
    CorrelationSketch c0sk = builder.build(fk, c0);
    CorrelationSketch c1sk = builder.build(fk, c1);
    CorrelationSketch c2sk = builder.build(fk, c2);

    double delta = 0.00001;

    assertThat(qsk.correlationTo(qsk).value).isCloseTo(1.0296530140645737, byLessThan(delta));
    assertThat(qsk.correlationTo(c0sk).value).isCloseTo(1.0296530140645737, byLessThan(delta));

    assertThat(qsk.correlationTo(c1sk).value).isCloseTo(0.3635634939595127, byLessThan(delta));
    assertThat(qsk.correlationTo(c2sk).value).isCloseTo(0.23185620475171878, byLessThan(delta));

    assertThat(c1sk.correlationTo(c2sk).value).isCloseTo(0.6206868526328018, byLessThan(delta));
  }
}
