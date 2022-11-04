package corrsketches.correlation;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.byLessThan;
import static org.junit.jupiter.api.Assertions.assertEquals;

import corrsketches.*;
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
    final Builder builder = CorrelationSketch.builder();

    List<String> pk = Arrays.asList("a", "b", "c", "d", "e");
    Column q = Column.numerical(1.0, 2.0, 3.0, 4.0, 5.0);
    CorrelationSketch qsk = builder.build(pk, q);

    List<String> c4fk = Arrays.asList("a", "b", "c", "z", "x");
    Column c4 = Column.numerical(1.0, 2.0, 3.0, 4.0, 5.0);
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
    Column q = Column.numerical(1.0, 2.0, 3.0, 4.0, 5.0);

    List<String> fk = Arrays.asList("a", "b", "c", "d", "e");
    Column c0 = Column.numerical(1.0, 2.0, 3.0, 4.0, 5.0);
    Column c1 = Column.numerical(1.1, 2.5, 3.0, 4.4, 5.9);
    Column c2 = Column.numerical(1.0, 3.2, 3.1, 4.9, 5.4);

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
    Column x = Column.numerical(-20., 21.0, 1.0, 1.0, 3.0, 4.0);

    List<String> ky = Arrays.asList("a", "b", "c", "d");
    Column ysum = Column.numerical(1.0, 2.0, 3.0, 4.0);
    Column ymean = Column.numerical(0.5, 1.0, 3.0, 4.0);
    Column ycount = Column.numerical(2.0, 2.0, 1.0, 1.0);

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
    Column q = Column.numerical(1.0, 2.0, 3.0, 4.0, 5.0);

    List<String> fk = Arrays.asList("a", "b", "c", "d", "e");
    Column c0 = Column.numerical(1.0, 2.0, 3.0, 4.0, 5.0);
    Column c1 = Column.numerical(1.1, 2.5, 3.0, 4.4, 5.9);
    Column c2 = Column.numerical(1.0, 3.2, 3.1, 4.9, 5.4);

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
    Column xvalues = Column.numerical(1.0, 2.0, 3.0, 4.0, 5.0);

    List<String> ykeys = Arrays.asList("!", "a", "b", "c", "d", "e");
    Column yvalues = Column.numerical(0.0, 2.0, 3.0, 4.0, 5.0, 6.0);

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
  public void shouldBeAbleToSampleRepeatedItems() {
    int[] xkeys = new int[] {1, 1, 1, 1, 1, 2, 2, 2, 3, 3, 4};
    Column xvalues = Column.numerical(1.0, 1.0, 1.0, 1.0, 1.0, 2.0, 2.0, 2.0, 3.0, 3.0, 4.0);
    Builder builder = CorrelationSketch.builder();
    int budget;

    // When a key is repeats frequently and no aggregation function is used, we expect that the
    // repeated key appear multiple times in the sketch. Their frequency in the sketch must be
    // proportional to their frequency in the original data.
    budget = xkeys.length;
    ImmutableCorrelationSketch xsi =
        builder
            .sketchType(SketchType.KMV, budget)
            .aggregateFunction(AggregateFunction.NONE)
            .build(xkeys, xvalues)
            .toImmutable();
    assertThat(xsi.getValues().length).isEqualTo(budget);
    assertThat(Arrays.stream(xsi.getKeys()).filter((it) -> it == 1).count()).isGreaterThan(1);
    assertThat(Arrays.stream(xsi.getKeys()).filter((it) -> it == 4).count()).isEqualTo(1);

    // We also require that the sketch must keep at least one entry of each unique key. Thus,
    // when the budget is equal to the number of distinct items, we expect that each key will be
    // present at least once in the sketch.
    budget = (int) Arrays.stream(xkeys).distinct().count();
    xsi =
        builder
            .aggregateFunction(AggregateFunction.NONE)
            .sketchType(SketchType.KMV, budget)
            .build(xkeys, xvalues)
            .toImmutable();
    assertThat(xsi.getValues().length).isGreaterThanOrEqualTo(budget);
    assertThat(xsi.getKeys().length).isGreaterThanOrEqualTo(budget);
    assertThat(xsi.getKeys().length).isGreaterThanOrEqualTo(budget);
    assertThat(Arrays.stream(xsi.getKeys()).distinct().toArray())
        .containsOnlyOnce(Arrays.stream(xkeys).distinct().toArray());
  }

  @Test
  public void shouldBeAbleToSampleRepeatedItemsAccordingToItsProbability() {
    int[] xkeys = new int[] {1, 1, 1, 1, 1, 1, 1, 2, 3, 4, 5, 6, 7};
    Column xvalues =
        Column.numerical(1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0);
    Builder builder = CorrelationSketch.builder();
    int budget;

    // When a key is repeats frequently and no aggregation function is used, we expect that the
    // repeated key appear multiple times in the sketch. Their frequency in the sketch must be
    // proportional to their frequency in the original data.
    budget = 4;
    ImmutableCorrelationSketch xsi =
        builder
            .sketchType(SketchType.KMV, budget)
            .aggregateFunction(AggregateFunction.NONE)
            .build(xkeys, xvalues)
            .toImmutable();
    System.out.println("xsi = " + Arrays.toString(xsi.getKeys()));
    System.out.println("xkeys.length = " + xkeys.length);
    System.out.println(
        "prob = " + Arrays.stream(xkeys).filter((it) -> it == 1).count() / (double) xkeys.length);

    // each key must be included at least once
    assertThat(Arrays.stream(xsi.getKeys()).filter((it) -> it == 4).count()).isEqualTo(1);
    // the probability of key 1 in the data is p≈0.538 and p*budget≈0.538*4 > 2, so the sketch
    // must contain more than one entry for the key 1.
    assertThat(Arrays.stream(xsi.getKeys()).filter((it) -> it == 1).count()).isEqualTo(2);
    // some items have high probability, so sketch must exceed the budget to accommodate such items
    assertThat(xsi.getValues().length).isGreaterThanOrEqualTo(budget);
  }

  @Test
  public void shouldCorrelationOverInnerJoinInSketchesWithRepeatedKeys() {
    int[] xkeys = new int[] {1, 1, 1, 1, 1, 1, 2, 2, 3, 4, 5, 6};
    Column xvalues = Column.numerical(1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 2.0, 2.0, 3.0, 4.0, 5, 6);

    final int budget = 5;
    final Builder builder = CorrelationSketch.builder().sketchType(SketchType.KMV, budget);
    CorrelationSketch lsk = builder.aggregateFunction(AggregateFunction.NONE).build(xkeys, xvalues);
    CorrelationSketch rsk = builder.aggregateFunction(AggregateFunction.MEAN).build(xkeys, xvalues);

    final ImmutableCorrelationSketch ilsk = lsk.toImmutable();
    final ImmutableCorrelationSketch irsk = rsk.toImmutable();

    assertThat(ilsk.getValues().length).isGreaterThan(budget);
    assertThat(irsk.getValues().length).isEqualTo(budget);

    final long countOfItem1inL = Arrays.stream(ilsk.getKeys()).filter(it -> it == 1).count();
    assertThat(countOfItem1inL).isGreaterThan(1);

    final long countOfItem1inR = Arrays.stream(irsk.getKeys()).filter(it -> it == 1).count();
    assertThat(countOfItem1inR).isEqualTo(1);

    final Join join = ilsk.join(irsk);
    final long countOfItem1inJoin = Arrays.stream(join.keys).filter(it -> it == 1).count();
    assertThat(countOfItem1inJoin).isEqualTo(countOfItem1inL * countOfItem1inR);

    final Estimate estimate = lsk.correlationTo(rsk, CorrelationType.PEARSONS.get());
    assertThat(estimate.value).isEqualTo(1.0);
    assertThat(estimate.sampleSize).isEqualTo(join.keys.length);

    final Estimate estimateImmutable = ilsk.correlationTo(irsk);
    assertThat(estimateImmutable.value).isEqualTo(estimate.value);
    assertThat(estimateImmutable.sampleSize).isEqualTo(estimate.sampleSize);
  }

  @Test
  public void sketchAggregationShouldHaveCorrectType1() {
    List<String> xkeys = Arrays.asList("a", "a", "a", "b", "b", "c");
    Column xvalues = Column.categorical(0, 0, 0, 0, 0, 0);

    List<String> ykeys = Arrays.asList("a", "b", "c");
    Column yvalues = Column.categorical(1, 2, 3);

    final Builder builder =
        CorrelationSketch.builder()
            .sketchType(SketchType.KMV, xkeys.size())
            .aggregateFunction(AggregateFunction.COUNT);

    CorrelationSketch xs = builder.build(xkeys, xvalues);
    CorrelationSketch ys = builder.build(ykeys, yvalues);

    final ImmutableCorrelationSketch xsi = xs.toImmutable();
    final ImmutableCorrelationSketch ysi = ys.toImmutable();

    // COUNT aggregation should change the output types to numerical
    assertEquals(ColumnType.NUMERICAL, xs.getOutputType());
    assertEquals(ColumnType.NUMERICAL, ys.getOutputType());

    final var join = xsi.join(ysi);
    assertEquals(3, join.keys.length);
    assertEquals(3, join.x.values.length);
    assertEquals(3, join.y.values.length);

    assertEquals(ColumnType.NUMERICAL, join.y.type);
    assertEquals(1, join.y.values[0]);
    assertEquals(1, join.y.values[1]);
    assertEquals(1, join.y.values[2]);

    assertEquals(ColumnType.NUMERICAL, join.x.type);
    assertThat(join.x.values).containsOnlyOnce(1);
    assertThat(join.x.values).containsOnlyOnce(2);
    assertThat(join.x.values).containsOnlyOnce(3);
  }

  @Test
  public void sketchAggregationShouldHaveCorrectType2() {
    List<String> xkeys = Arrays.asList("a", "a", "a", "b", "b", "b", "c", "c", "c");
    Column xvalues = Column.categorical(2, 2, 0, 2, 2, 1, 2, 2, 0);

    List<String> ykeys = Arrays.asList("a", "b", "c");
    Column yvalues = Column.categorical(1, 2, 3);

    final Builder builder =
        CorrelationSketch.builder()
            .sketchType(SketchType.KMV, xkeys.size())
            .aggregateFunction(AggregateFunction.MOST_FREQUENT);

    // when
    CorrelationSketch ys = builder.build(ykeys, yvalues);
    CorrelationSketch xs = builder.build(xkeys, xvalues);
    final ImmutableCorrelationSketch ysi = ys.toImmutable();
    final ImmutableCorrelationSketch xsi = xs.toImmutable();

    // then

    // MOST_FREQUENT aggregation should maintain the input type
    assertEquals(ColumnType.CATEGORICAL, ys.getOutputType());
    assertEquals(ColumnType.CATEGORICAL, xs.getOutputType());

    assertThat(ysi.getValues()).containsOnlyOnce(1);
    assertThat(ysi.getValues()).containsOnlyOnce(2);
    assertThat(ysi.getValues()).containsOnlyOnce(3);

    assertEquals(2, xsi.getValues()[0]);
    assertEquals(2, xsi.getValues()[1]);
    assertEquals(2, xsi.getValues()[2]);

    // when
    final var join = xsi.join(ysi);
    // then
    assertEquals(3, join.x.values.length);
    assertEquals(3, join.y.values.length);
    assertEquals(ColumnType.CATEGORICAL, join.x.type);
    assertEquals(2, join.x.values[0]);
    assertEquals(2, join.x.values[1]);
    assertEquals(2, join.x.values[2]);
    assertEquals(ColumnType.CATEGORICAL, join.y.type);
    assertThat(join.y.values).containsOnlyOnce(1);
    assertThat(join.y.values).containsOnlyOnce(2);
    assertThat(join.y.values).containsOnlyOnce(3);
  }

  @Test
  public void shouldComputeCorrelationUsingImmutableSketchOnRandomVectors() {
    Random r = new Random();

    int runs = 100;
    long[] runningTimes = new long[runs];
    long[] runningTimesImmutable = new long[runs];

    for (int i = 0; i < runs; i++) {
      final double jc = r.nextDouble();
      final int n = 10_000;
      double[] y = new double[n];
      double[] x = new double[n];
      String[] kx = new String[n];
      String[] ky = new String[n];
      for (int j = 0; j < n; j++) {
        x[j] = r.nextGaussian() * (1_000_000);
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

      CorrelationSketch xsketch = builder.build(kx, Column.numerical(x));
      CorrelationSketch ysketch = builder.build(ky, Column.numerical(y));

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
    Column q = Column.categorical(1, 1, 1, 2, 2, 2, 2, 2, 3, 3);

    List<String> fk = Arrays.asList("a", "b", "c", "d", "e", "f", "g", "h", "i", "j");
    Column c0 = Column.categorical(1, 1, 1, 2, 2, 2, 2, 2, 3, 3);
    Column c1 = Column.categorical(1, 2, 2, 3, 2, 3, 2, 3, 1, 2);
    Column c2 = Column.categorical(1, 2, 2, 1, 2, 3, 2, 3, 2, 2);

    final Builder builder =
        CorrelationSketch.builder().estimator(CorrelationType.MUTUAL_INFORMATION_DIFF_ENT);

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

  @Test
  public void shouldEstimateMutualInformationForMixedDataTypes() {
    List<String> pk = Arrays.asList("a", "b", "c", "d", "e", "f", "g", "h", "i", "j");
    Column xn = Column.numerical(1, 1, 1, 2, 2, 2, 2, 2, 3, 3);
    Column xc = Column.categorical(1, 1, 1, 2, 2, 2, 2, 2, 3, 3);

    List<String> fk = Arrays.asList("a", "b", "c", "d", "e", "f", "g", "h", "i", "j");
    Column yc = Column.categorical(1, 1, 1, 2, 2, 2, 2, 2, 3, 3);
    Column yn = Column.numerical(1, 2, 2, 3, 2, 3, 2, 3, 1, 2);

    final Builder builder =
        CorrelationSketch.builder().estimator(CorrelationType.MUTUAL_INFORMATION_DIFF_ENT);

    CorrelationSketch xnsk = builder.build(pk, xn);
    CorrelationSketch xcsk = builder.build(pk, xc);
    CorrelationSketch ycsk = builder.build(fk, yc);
    CorrelationSketch ynsk = builder.build(fk, yn);

    double delta = 0.00000000001;

    assertThat(xcsk.correlationTo(ycsk).value).isCloseTo(1.0296530140645737, byLessThan(delta));
    assertThat(xcsk.correlationTo(ynsk).value).isCloseTo(0.027301587301587604, byLessThan(delta));

    assertThat(xnsk.correlationTo(ycsk).value).isCloseTo(1.1373015873015877, byLessThan(delta));
    assertThat(xnsk.correlationTo(ynsk).value).isCloseTo(0.9964674245622457, byLessThan(delta));
  }
}
