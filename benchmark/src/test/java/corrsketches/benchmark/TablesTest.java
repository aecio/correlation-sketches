package corrsketches.benchmark;

import static org.junit.jupiter.api.Assertions.*;

import corrsketches.aggregations.AggregateFunction;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

public class TablesTest {

  @Test
  public void shouldAggregateColumnPairWithRepeatedKeys() {
    // given
    List<String> key = Arrays.asList("a", "b", "b", "c", "d", "d", "d");
    // mean: a=1, b=2, c=3, d=4
    double[] values = new double[] {1.0, 1.0, 3.0, 3.0, 0.0, 8.0, 4.0};

    final ColumnPair cp = new ColumnPair("B", "fk_b", key, "values_b", values);
    final List<AggregateFunction> functions =
        Arrays.asList(AggregateFunction.MEAN, AggregateFunction.COUNT);

    // when
    final List<ColumnPair> columnPairs = JoinAggregation.aggregateColumnPair(cp, functions);
    ColumnPair mean = columnPairs.get(0);
    ColumnPair count = columnPairs.get(1);

    // then
    assertEquals(4, mean.keyValues.size());
    assertEquals(4, mean.columnValues.length);
    assertEquals(1.0, getValueOfKey(mean, "a"));
    assertEquals(2.0, getValueOfKey(mean, "b"));
    assertEquals(3.0, getValueOfKey(mean, "c"));
    assertEquals(4.0, getValueOfKey(mean, "d"));

    assertEquals(4, count.keyValues.size());
    assertEquals(4, count.columnValues.length);
    assertEquals(1.0, getValueOfKey(count, "a"));
    assertEquals(2.0, getValueOfKey(count, "b"));
    assertEquals(1.0, getValueOfKey(count, "c"));
    assertEquals(3.0, getValueOfKey(count, "d"));
  }

  @Test
  public void shouldAggregateColumnPairWithUniqueKeys() {
    // given
    List<String> keys = Arrays.asList("a", "b", "c", "d", "e");
    double[] values = new double[] {1.0, 2.0, 3.0, 4.0, 5.0};

    final ColumnPair cp = new ColumnPair("B", "fk_b", keys, "values_b", values);
    final List<AggregateFunction> functions =
        Arrays.asList(AggregateFunction.MEAN, AggregateFunction.COUNT);

    // when
    final List<ColumnPair> columnPairs = JoinAggregation.aggregateColumnPair(cp, functions);
    ColumnPair mean = columnPairs.get(0);
    ColumnPair count = columnPairs.get(1);

    // then
    assertEquals(5, mean.keyValues.size());
    assertEquals(5, mean.columnValues.length);
    assertEquals(1.0, getValueOfKey(mean, "a"));
    assertEquals(2.0, getValueOfKey(mean, "b"));
    assertEquals(3.0, getValueOfKey(mean, "c"));
    assertEquals(4.0, getValueOfKey(mean, "d"));
    assertEquals(5.0, getValueOfKey(mean, "e"));

    assertEquals(5, count.keyValues.size());
    assertEquals(5, count.columnValues.length);
    assertEquals(1.0, getValueOfKey(count, "a"));
    assertEquals(1.0, getValueOfKey(count, "b"));
    assertEquals(1.0, getValueOfKey(count, "c"));
    assertEquals(1.0, getValueOfKey(count, "d"));
    assertEquals(1.0, getValueOfKey(count, "e"));
  }

  private double getValueOfKey(ColumnPair aggregate, String key) {
    for (int i = 0; i < aggregate.keyValues.size(); i++) {
      if (aggregate.keyValues.get(i).equals(key)) {
        return aggregate.columnValues[i];
      }
    }
    fail("Could not find key value: " + key);
    return 0.0; // this will never be executed
  }

  @Test
  public void shouldJoinTablesAndComputeCorrelation() {
    List<String> keyA = Arrays.asList("a", "b", "c", "d", "e");
    double[] valuesA = new double[] {1.0, 2.0, 3.0, 4.0, 5.0};

    List<String> keyB = Arrays.asList(new String[] {"a", "b", "c", "d"});
    double[] valuesB = new double[] {1.0, 2.0, 3.0, 4.0};

    List<String> keyC = Arrays.asList(new String[] {"a", "b", "c", "z", "x"});
    double[] valuesC = new double[] {0., 0., 3.0, 4.0, 5.0};

    List<String> keyD = Arrays.asList(new String[] {"a", "b", "c", "z"});
    double[] valuesD = new double[] {-1., -2., -3.0, 4.0};

    ColumnPair columnA = new ColumnPair("A", "pk_a", keyA, "values_a", valuesA);
    ColumnPair columnB = new ColumnPair("B", "fk_b", keyB, "values_b", valuesB);
    ColumnPair columnC = new ColumnPair("C", "fk_c", keyC, "values_c", valuesC);
    ColumnPair columnD = new ColumnPair("D", "fk_d", keyD, "values_d", valuesD);

    double delta = 0.0001;
    assertEquals(1.000, getPearsonCorrelation(columnA, columnB), delta);
    assertEquals(1.000, getPearsonCorrelation(columnB, columnA), delta);

    assertEquals(0.866, getPearsonCorrelation(columnB, columnC), delta);
    assertEquals(0.866, getPearsonCorrelation(columnC, columnB), delta);

    assertEquals(-1.000, getPearsonCorrelation(columnB, columnD), delta);
    assertEquals(-1.000, getPearsonCorrelation(columnD, columnB), delta);
  }

  @Test
  //  @Disabled
  @DisplayName("should join and aggregate tables before computing correlations")
  public void shouldJoinAndAggregate() {
    List<String> keyA = Arrays.asList("a", "b", "c", "d", "e");
    double[] valuesA = new double[] {1.0, 2.0, 3.0, 4.0, 5.0};

    List<String> keyB = Arrays.asList("a", "b", "b", "c", "d", "d");
    // mean: a=1, b=2, d=4
    double[] valuesB = new double[] {1.0, 1.0, 3.0, 3.0, 0.0, 8.0};

    ColumnPair columnA = new ColumnPair("A", "pk_a", keyA, "values_a", valuesA);
    ColumnPair columnB = new ColumnPair("B", "fk_b", keyB, "values_b", valuesB);

    double delta = 0.0001;
    assertEquals(1.000, getPearsonCorrelation(columnA, columnB), delta);
    assertEquals(1.000, getPearsonCorrelation(columnB, columnA), delta);
  }

  private double getPearsonCorrelation(ColumnPair columnA, ColumnPair columnB) {
    return BenchmarkUtils.computeCorrelationsAfterJoin(
            columnA,
            columnB,
            Collections.singletonList(AggregateFunction.MEAN),
            new MetricsResult())
        .get(0)
        .corr_rp_actual;
  }
}
