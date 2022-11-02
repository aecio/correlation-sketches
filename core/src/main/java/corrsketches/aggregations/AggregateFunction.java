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
  MOST_FREQUENT(MostFrequent::new),
  SAMPLER(() -> null);

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
    fn.first(x[0]);
    for (int i = 1; i < length; i++) {
      fn.update(x[i]);
    }
    return fn.aggregatedValue();
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

    void first(double value);

    void update(double current);

    double aggregatedValue();

    ColumnType getOutputType(ColumnType columnValueType);
  }

  public abstract static class BaseAggregator implements Aggregator {
    double previous = 0d;

    @Override
    public void first(double value) {
      this.previous = value;
    }

    @Override
    public double aggregatedValue() {
      return previous;
    }
  }

  /** Common interface for aggregators that always return numerical aggregated values. */
  public abstract static class NumberAggregator extends BaseAggregator {
    public ColumnType getOutputType(ColumnType columnValueType) {
      return ColumnType.NUMERICAL;
    }
  }

  /**
   * Common interface for aggregators that aggregated values of the same type of the input column.
   */
  public abstract static class SameTypeAggregator extends BaseAggregator {
    public ColumnType getOutputType(ColumnType columnValueType) {
      return columnValueType;
    }
  }

  /* Number aggregators */

  static class SumNumberAggregator extends NumberAggregator {
    @Override
    public void update(double current) {
      super.previous += current;
    }
  }

  static class MaxNumberAggregator extends NumberAggregator {
    @Override
    public void update(double current) {
      super.previous = Math.max(super.previous, current);
    }
  }

  static class MinNumberAggregator extends NumberAggregator {
    @Override
    public void update(double current) {
      super.previous = Math.min(super.previous, current);
    }
  }

  /* Same-type aggregators */

  static class FirstAggregator extends SameTypeAggregator {
    @Override
    public void update(double current) {
      // no-op: previous value must be equal to the first as implemented in the base class
    }
  }

  static class LastAggregator extends SameTypeAggregator {
    @Override
    public void update(double current) {
      super.previous = current;
    }
  }

  private static class Mean extends NumberAggregator {

    int n = 1;

    @Override
    public void update(double current) {
      n++;
      super.previous = super.previous + ((current - previous) / n);
    }
  }

  private static class Count extends NumberAggregator {

    double previous;

    @Override
    public void first(double value) {
      previous = 1;
    }

    @Override
    public void update(double current) {
      this.previous++;
    }
  }

  private static class MostFrequent extends SameTypeAggregator {

    public Int2IntMap counts = new Int2IntOpenHashMap();

    @Override
    public void first(double value) {
      this.update(value);
    }

    @Override
    public void update(double current) {
      final int key = (int) current;
      int currentCount = counts.getOrDefault(key, 0);
      counts.put(key, currentCount + 1);
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
