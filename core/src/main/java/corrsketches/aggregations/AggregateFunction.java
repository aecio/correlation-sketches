package corrsketches.aggregations;

import corrsketches.ColumnType;
import it.unimi.dsi.fastutil.doubles.DoubleArrayList;
import it.unimi.dsi.fastutil.doubles.DoubleList;
import it.unimi.dsi.fastutil.ints.Int2IntMap;
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import java.util.Arrays;
import java.util.List;

public enum AggregateFunction {
  FIRST(FirstAggregator::new),
  LAST(LastAggregator::new),
  MAX(MaxNumberAggregator::new),
  MIN(MinNumberAggregator::new),
  SUM(SumNumberAggregator::new),
  MEAN(Mean::new),
  COUNT(Count::new),
  MOST_FREQUENT(MostFrequent::new),
  NONE(NoneAggregator::new);

  private final AggregatorProvider provider;

  AggregateFunction(AggregatorProvider provider) {
    this.provider = provider;
  }

  public AggregatorProvider getProvider() {
    return provider;
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
    final RepeatedValueHandler fn = provider.get();
    fn.first(x[0]);
    for (int i = 1; i < length; i++) {
      fn.update(x[i]);
    }
    return fn.aggregatedValue();
  }

  interface AggregatorProvider extends RepeatedValueHandlerProvider {

    default RepeatedValueHandler create() {
      return get();
    }

    Aggregator get();
  }

  /** Common interface for all double number aggregators. */
  public interface Aggregator extends RepeatedValueHandler {

    @Override
    default boolean isAggregator() {
      return true;
    }

    @Override
    default DoubleList values() {
      return DoubleList.of(aggregatedValue());
    }

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

    @Override
    public boolean acceptsInputColumnType(ColumnType inputDataType) {
      return inputDataType == ColumnType.NUMERICAL;
    }
  }

  /**
   * Common interface for aggregators that aggregated values of the same type of the input column.
   */
  public abstract static class SameTypeAggregator extends BaseAggregator {
    public ColumnType getOutputType(ColumnType columnValueType) {
      return columnValueType;
    }

    @Override
    public boolean acceptsInputColumnType(ColumnType inputDataType) {
      return true;
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

  public static class NoneAggregator extends SameTypeAggregator {
    @Override
    public void update(double current) {
      throw new IllegalStateException("update() shouldn't be called when NONE aggregator is used.");
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

    int count = 0;

    @Override
    public void first(double value) {
      this.count++;
    }

    @Override
    public void update(double current) {
      this.count++;
    }

    @Override
    public double aggregatedValue() {
      return this.count;
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

    @Override
    public boolean acceptsInputColumnType(ColumnType inputDataType) {
      return inputDataType == ColumnType.CATEGORICAL;
    }
  }
}
