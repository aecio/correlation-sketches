package corrsketches.aggregations;

import it.unimi.dsi.fastutil.doubles.DoubleArrayList;
import it.unimi.dsi.fastutil.ints.Int2IntMap;
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import java.util.Arrays;
import java.util.List;

public enum AggregateFunction {
  FIRST((previous, current) -> previous),
  LAST((previous, current) -> current),
  MAX(Math::max),
  MIN(Math::min),
  SUM(Double::sum),
  MEAN(Mean::new),
  COUNT(Count::new),
  MOST_FREQUENT(MostFrequent::new);

  private final AggregatorProvider provider;

  AggregateFunction(Aggregator function) {
    this(() -> function);
  }

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

  /** Common interface for all double number aggregators. */
  public interface Aggregator {

    default double first(double value) {
      return value;
    }

    double update(double previous, double current);
  }

  public interface BatchAggregator extends Aggregator {
    public double aggregatedValue();
  }

  /** Creates an instance of an aggregator interface. */
  private interface AggregatorProvider {
    Aggregator get();
  }

  private static class Mean implements Aggregator {

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
  }

  private static class MostFrequent implements BatchAggregator {

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
