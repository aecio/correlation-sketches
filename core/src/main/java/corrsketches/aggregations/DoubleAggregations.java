package corrsketches.aggregations;

import corrsketches.aggregations.AggregateFunction.Aggregator;
import java.util.List;

public class DoubleAggregations {

  AggregateFunction[] functions;
  double[] values;

  private DoubleAggregations() {}

  public static DoubleAggregations aggregate(double[] x, List<AggregateFunction> functions) {
    assert x.length > 0;

    DoubleAggregations aggregations = new DoubleAggregations();
    aggregations.values = new double[functions.size()];
    aggregations.functions = new AggregateFunction[functions.size()];

    for (int i = 0; i < functions.size(); i++) {
      Aggregator aggregator = functions.get(i).get();
      double aggregate = aggregator.first(x[0]);
      for (int xidx = 1; xidx < x.length; xidx++) {
        aggregate = aggregator.update(aggregate, x[xidx]);
      }
      aggregations.values[i] = aggregate;
      aggregations.functions[i] = functions.get(i);
    }
    return aggregations;
  }

  public double get(AggregateFunction function) {
    for (int i = 0; i < functions.length; i++) {
      if (function.equals(this.functions[i])) {
        return this.values[i];
      }
    }
    throw new IllegalArgumentException("Given aggregation function not computed");
  }
}
