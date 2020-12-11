package corrsketches.aggregations;

import java.util.Arrays;
import java.util.List;

public enum AggregateFunction {
  FIRST((previous, current) -> previous),
  LAST((previous, current) -> current),
  MAX(Math::max),
  MIN(Math::min),
  SUM(((previous, current) -> previous + current)),
  MEAN(() -> new Mean()),
  COUNT(() -> new Count());

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
    Aggregator fn = provider.get();
    double aggregate = fn.first(x[0]);
    for (int i = 1; i < x.length; i++) {
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
      final double avg = previous + ((current - previous) / n);
      return avg;
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
