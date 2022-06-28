package corrsketches.aggregations;

import corrsketches.ColumnType;
import it.unimi.dsi.fastutil.doubles.DoubleArrayList;
import it.unimi.dsi.fastutil.ints.Int2IntMap;
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import java.util.Arrays;
import java.util.List;

public enum AggregateFunction {
  FIRST(new SingletonAggregator(FirstAggregator::new)),
  LAST(new SingletonAggregator(LastAggregator::new)),
  MAX(new SingletonAggregator(MaxNumberAggregator::new)),
  MIN(new SingletonAggregator(MinNumberAggregator::new)),
  SUM(new SingletonAggregator(SumNumberAggregator::new)),
  MEAN(Mean::new),
  COUNT(Count::new),
  MOST_FREQUENT(MostFrequent::new);

  private final AggregatorProvider provider;

  AggregateFunction(AggregatorProvider provider) {
    this.provider = provider;
  }

  public Aggregator get() {
    return provider.get();
  }

  public static List<AggregateFunction> all() {
    return Arrays.asList(FIRST, LAST, MAX, MIN, SUM, MEAN, COUNT);
  }

  public double aggregate(double[] x) {
    return aggregate(x, x.length);
  }

  public double aggregate(DoubleArrayList value) {
    return aggregate(value.elements(), value.size());
  }

  public double aggregate(double[] x, int length) {
    final Aggregator fn = provider.get();
    double aggregate = fn.first(x[0]);
    for (int i = 1; i < length; i++) {
      aggregate = fn.update(aggregate, x[i]);
    }
    if (fn instanceof BatchAggregator) {
      aggregate = ((BatchAggregator) fn).aggregatedValue();
    }
    return aggregate;
  }

  /** Creates an instance of an aggregator interface. */
  private interface AggregatorProvider {
    Aggregator get();
  }

  /**
   * An aggregator provider for stateless aggregators that always returns the same aggregator
   * instance.
   */
  static class SingletonAggregator implements AggregatorProvider {
    private final Aggregator aggregator;

    SingletonAggregator(AggregatorProvider aggregatorProvider) {
      this.aggregator = aggregatorProvider.get();
    }

    @Override
    public Aggregator get() {
      return aggregator;
    }
  }

  /** Common interface for all double number aggregators. */
  public interface Aggregator {

    default double first(double value) {
      return value;
    }

    double update(double previous, double current);

    ColumnType getOutputType(ColumnType columnValueType);
  }

  /**
   * Common interface for aggregators that only work in batch, i.e., the aggregated value needs to
   * be computed using provided method aggregatedValue().
   */
  public interface BatchAggregator extends Aggregator {
    double aggregatedValue();
  }

  /** Common interface for aggregators that always return numerical aggregated values. */
  public interface NumberAggregator extends Aggregator {
    default ColumnType getOutputType(ColumnType columnValueType) {
      return ColumnType.NUMERICAL;
    }
  }

  /**
   * Common interface for aggregators that aggregated values of the same type of the input column.
   */
  public interface SameTypeAggregator extends Aggregator {
    default ColumnType getOutputType(ColumnType columnValueType) {
      return columnValueType;
    }
  }

  /* Number aggregators */

  static class SumNumberAggregator implements NumberAggregator {
    @Override
    public double update(double previous, double current) {
      return previous + current;
    }
  }

  static class MaxNumberAggregator implements NumberAggregator {
    @Override
    public double update(double previous, double current) {
      return Math.max(previous, current);
    }
  }

  static class MinNumberAggregator implements NumberAggregator {
    @Override
    public double update(double previous, double current) {
      return Math.min(previous, current);
    }
  }

  /* Same-type aggregators */

  static class FirstAggregator implements SameTypeAggregator {
    @Override
    public double update(double previous, double current) {
      return previous;
    }
  }

  static class LastAggregator implements SameTypeAggregator {
    @Override
    public double update(double previous, double current) {
      return current;
    }
  }

  private static class Mean implements NumberAggregator {

    int n = 1;

    @Override
    public double update(double previous, double current) {
      n++;
      return previous + ((current - previous) / n);
    }
  }

  private static class Count implements Aggregator {

    @Override
    public double first(double value) {
      return 1;
    }

    @Override
    public double update(double previous, double current) {
      return previous + 1;
    }

    public ColumnType getOutputType(ColumnType columnValueType) {
      return ColumnType.NUMERICAL;
    }
  }

  private static class MostFrequent implements BatchAggregator, SameTypeAggregator {

    public Int2IntMap counts = new Int2IntOpenHashMap();

    @Override
    public double first(double value) {
      return update(0, value);
    }

    @Override
    public double update(double previous, double current) {
      final int key = (int) current;
      int currentCount = counts.getOrDefault(key, 0);
      counts.put(key, currentCount + 1);
      return 1;
    }

    @Override
    public double aggregatedValue() {
      int max = -1;
      int maxItem = -1;
      for (var kv : counts.int2IntEntrySet()) {
        if (kv.getIntValue() > max) {
          max = kv.getIntValue();
          maxItem = kv.getIntKey();
        }
      }
      return maxItem;
    }
  }
}
