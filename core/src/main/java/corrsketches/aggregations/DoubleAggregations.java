package corrsketches.aggregations;

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
      final AggregateFunction fn = functions.get(i);
      aggregations.values[i] = fn.aggregate(x);
      aggregations.functions[i] = fn;
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
