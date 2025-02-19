package corrsketches.aggregations;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;

import java.util.Collections;
import java.util.List;
import org.junit.jupiter.api.Test;

class DoubleAggregationsTest {

  @Test
  public void shouldNotReuseAggregateFunctionInstance() {
    assertNotSame(AggregateFunction.FIRST.get(), AggregateFunction.FIRST.get());
    assertNotSame(AggregateFunction.LAST.get(), AggregateFunction.LAST.get());
    assertNotSame(AggregateFunction.MAX.get(), AggregateFunction.MAX.get());
    assertNotSame(AggregateFunction.MIN.get(), AggregateFunction.MIN.get());
    assertNotSame(AggregateFunction.SUM.get(), AggregateFunction.SUM.get());
    assertNotSame(AggregateFunction.MEAN.get(), AggregateFunction.MEAN.get());
    assertNotSame(AggregateFunction.COUNT.get(), AggregateFunction.COUNT.get());
    assertNotSame(AggregateFunction.MOST_FREQUENT.get(), AggregateFunction.MOST_FREQUENT.get());
  }

  @Test
  public void shouldAggregateDoubleArray() {
    double[] x = {0, 1, 1, 2};

    DoubleAggregations aggregations = DoubleAggregations.aggregate(x, AggregateFunction.all());

    assertEquals(0, aggregations.get(AggregateFunction.FIRST));
    assertEquals(2, aggregations.get(AggregateFunction.LAST));
    assertEquals(0, aggregations.get(AggregateFunction.MIN));
    assertEquals(2, aggregations.get(AggregateFunction.MAX));
    assertEquals(4, aggregations.get(AggregateFunction.SUM));
    assertEquals((0 + 1 + 1 + 2) / 4.0, aggregations.get(AggregateFunction.MEAN));
    assertEquals(4, aggregations.get(AggregateFunction.COUNT));
  }

  @Test
  public void shouldComputeMeanCorrectly() {
    final List<AggregateFunction> mean = Collections.singletonList(AggregateFunction.MEAN);
    double[] x = {1, 2, 3};

    DoubleAggregations aggregations = DoubleAggregations.aggregate(x, mean);
    assertEquals(2, aggregations.get(AggregateFunction.MEAN));

    x = new double[] {-1, 0, 1};
    aggregations = DoubleAggregations.aggregate(x, mean);
    assertEquals(0, aggregations.get(AggregateFunction.MEAN));

    x = new double[] {-32, 0, 9, 23};
    aggregations = DoubleAggregations.aggregate(x, mean);
    assertEquals((-32 + 0 + 9 + 23) / 4.0, aggregations.get(AggregateFunction.MEAN));
  }

  @Test
  public void shouldComputeMostFrequentInt() {
    final List<AggregateFunction> mf = Collections.singletonList(AggregateFunction.MOST_FREQUENT);
    double[] x = {1, 2, 2, 2, 3, 3};

    DoubleAggregations aggregations = DoubleAggregations.aggregate(x, mf);
    assertEquals(2, aggregations.get(AggregateFunction.MOST_FREQUENT));

    x = new double[] {-1, -1, 0, 0, 0, 0, 1};
    aggregations = DoubleAggregations.aggregate(x, mf);
    assertEquals(0, aggregations.get(AggregateFunction.MOST_FREQUENT));
  }
}
