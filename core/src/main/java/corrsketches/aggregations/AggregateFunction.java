package corrsketches.aggregations;

import it.unimi.dsi.fastutil.doubles.DoubleArrayList;
import java.util.Arrays;
import java.util.List;

public enum AggregateFunction {
  FIRST((previous, current) -> previous),
  LAST((previous, current) -> current),
  MAX(Math::max),
  MIN(Math::min),
  SUM(Double::sum),
  MEAN(Mean::new),
  COUNT(Count::new);

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
    return aggregate;
  }

  /** Common interface for all double number aggregators. */
  public interface Aggregator {

    default double first(double value) {
      return value;
    }

    double update(double previous, double current);
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
}
