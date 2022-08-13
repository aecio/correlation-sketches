package corrsketches.benchmark;

import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.*;

import corrsketches.Column;
import corrsketches.ColumnType;
import corrsketches.CorrelationSketch;
import corrsketches.CorrelationSketch.Builder;
import corrsketches.CorrelationSketch.ImmutableCorrelationSketch.Join;
import corrsketches.SketchType;
import corrsketches.aggregations.AggregateFunction;
import corrsketches.benchmark.CategoricalJoinAggregation.Aggregation;
import corrsketches.correlation.CorrelationType;
import corrsketches.correlation.MutualInformation;
import corrsketches.util.Hashes;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.junit.jupiter.api.Test;
import tech.tablesaw.api.DoubleColumn;
import tech.tablesaw.api.IntColumn;
import tech.tablesaw.api.StringColumn;
import tech.tablesaw.api.Table;

public class MutualInformationSketchTest {

  @Test
  public void shouldEstimateMutualInformation() {
    List<String> pk = asList("a", "b", "c", "d", "e", "f", "g", "h", "i", "j");
    Column q = Column.categorical(1, 1, 1, 2, 2, 2, 2, 2, 3, 3);

    List<String> fk = asList("a", "b", "c", "d", "e", "f", "g", "h", "i", "j");
    Column c0 = Column.categorical(1, 1, 1, 2, 2, 2, 2, 2, 3, 3);
    Column c1 = Column.categorical(1, 2, 2, 3, 2, 3, 2, 3, 1, 2);
    Column c2 = Column.categorical(1, 2, 2, 1, 2, 3, 2, 3, 2, 2);

    Builder builder =
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
  public void shouldEstimateMutualInformationAfterJoin() {
    List<String> pk = asList("a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m");
    Column x = Column.categorical(1, 1, 1, 2, 2, 2, 2, 2, 2, 2, 3, 1, 2);

    List<String> fk = asList("e", "e", "e", "f", "g", "h", "i", "j", "k", "l", "m", "n", "o");
    Column y = Column.categorical(1, 2, 2, 3, 2, 2, 3, 1, 2, 3, 2, 3, 1);

    Builder builder =
        CorrelationSketch.builder()
            .sketchType(SketchType.KMV, 6)
            .aggregateFunction(AggregateFunction.MOST_FREQUENT)
            .estimator(CorrelationType.MUTUAL_INFORMATION_DIFF_ENT);

    CorrelationSketch xsk = builder.build(pk, x);
    CorrelationSketch ysk = builder.build(fk, y);

    Table tx =
        Table.create(
            "T_X",
            StringColumn.create("PK", pk),
            IntColumn.create("h(PK)", pk.stream().mapToInt(Hashes::murmur3_32).toArray()),
            DoubleColumn.create("X", x.values).asIntColumn());
    System.out.println(tx);

    System.out.println();
    Table ty =
        Table.create(
            "T_Y",
            StringColumn.create("FK", fk),
            IntColumn.create("h(FK)", fk.stream().mapToInt(Hashes::murmur3_32).toArray()),
            DoubleColumn.create("Y", y.values).asIntColumn());
    System.out.println(ty);

    //
    // GROUND TRUTH TABLE
    //
    ColumnType valueType = ColumnType.CATEGORICAL;
    ColumnPair cpx = new ColumnPair("TX", "PK", pk, "X", valueType, x.values);
    ColumnPair cpy = new ColumnPair("TY", "FK", fk, "Y", valueType, y.values);
    Aggregation join =
        CategoricalJoinAggregation.leftJoinAggregate(
                cpy, cpx, Collections.singletonList(AggregateFunction.MOST_FREQUENT))
            .get(0);

    System.out.println();
    Table tlj =
        Table.create(
            "Left Join (Left=Y, right=X)",
            StringColumn.create("FK", join.keys),
            DoubleColumn.create("Y", join.a.values).asIntColumn(),
            DoubleColumn.create("X", join.b.values).asIntColumn());
    System.out.println(tlj);
    System.out.println("Y: " + Arrays.toString(join.a.values));
    System.out.println("X: " + Arrays.toString(join.b.values));
    System.out.printf(
        "MI(Y,X) = %.4f\n", MutualInformation.estimateMi(join.a.values, join.b.values).value);

    //
    // SKETCH INTERSECTION TABLE
    //

    System.out.println();
    Join sketchJoin = xsk.toImmutable().join(ysk.toImmutable());
    Table df =
        Table.create(
            "Sketch intersection table",
            IntColumn.create("PK", sketchJoin.keys),
            DoubleColumn.create("agg(X)", sketchJoin.x.values).asIntColumn(),
            DoubleColumn.create("agg(Y)", sketchJoin.y.values).asIntColumn());
    System.out.println(df);
    System.out.printf(
        "MI(agg(X), agg(Y)) = %.4f\n",
        MutualInformation.estimateMi(sketchJoin.x.values, sketchJoin.y.values).value);

    double delta = 0.1;
    assertThat(xsk.correlationTo(ysk).value).isCloseTo(0.1808106406067122, byLessThan(delta));
  }

  //  @Test
  //  public void testMutualInformationForMixedVariables() {
  //    // ground-truth values computed using scikit-learn mutual_info_classif
  //    List<String> pk = asList("a", "b", "c", "d", "e", "f");
  //    Column x1 = Column.numerical(1, 2, 4, 8, 16, 32);
  //    Column x2 = Column.numerical(2, 3, 4, 8, 16, 32);
  //
  //    List<String> fk = asList("a", "b", "c", "d", "e", "f");
  //    Column y = Column.categorical(1, 1, 2, 2, 3, 3);
  //
  //    Builder builder = CorrelationSketch.builder().estimator(CorrelationType.MUTUAL_INFORMATION);
  //
  //    CorrelationSketch x1sk = builder.build(pk, x1);
  //    CorrelationSketch x2sk = builder.build(pk, x2);
  //    CorrelationSketch ysk = builder.build(fk, y);
  //    //    CorrelationSketch c1sk = builder.build(fk, c1);
  //    //    CorrelationSketch c2sk = builder.build(fk, c2);
  //
  //    double delta = 0.00001;
  //    //
  //    int n = 10;
  //    double[] z1 = new double[n];
  //    double[] z2 = new double[n];
  //    for (int i = 0; i < n; i++) {
  //      z1[i] = ysk.correlationTo(x1sk).value;
  //      z2[i] = ysk.correlationTo(x2sk).value;
  //    }
  //    System.out.println(Arrays.toString(z1));
  //
  //    assertThat(Stats.mean(z1)).isCloseTo(0.58933333, byLessThan(delta));
  //    assertThat(Stats.mean(z2)).isCloseTo(0.39361111, byLessThan(delta));
  //    //    assertThat(ysk.correlationTo(x2sk).value).isCloseTo(0.39361111, byLessThan(delta));
  //    //
  //    //    assertThat(DifferentialEntropy.entropy(q)).isEqualTo(3.218266805508045,
  // within(DELTA));
  //    //
  //    ////    double[] a = new double[] {1, 2, 4, 8, 16};
  //    //    assertThat(DifferentialEntropy.entropy(a)).isEqualTo(3.218266805508045,
  // within(DELTA));
  //    //
  //    //    double[] b = new double[] {1, 2, 1, 2, 1.2};
  //    //    assertThat(DifferentialEntropy.entropy(b)).isCloseTo(1.2318518036304367,
  // within(DELTA));
  //    //
  //    //    double[] c =
  //    //        new double[] {
  //    //          -0.59152691,
  //    //          -0.21027888,
  //    //          1.40407995,
  //    //          -0.53021491,
  //    //          0.58272939,
  //    //          -0.23601182,
  //    //          -1.19971974,
  //    //          -1.50147482,
  //    //          0.25556115,
  //    //          -0.06472547,
  //    //          -0.56735615,
  //    //          -0.38815229,
  //    //          -1.10666078,
  //    //          -0.26985764,
  //    //          0.1365975
  //    //        };
  //    //    assertThat(DifferentialEntropy.entropy(c)).isCloseTo(1.2231987815353995,
  // within(DELTA));
  //  }
}
