package corrsketches.benchmark;

import corrsketches.aggregations.AggregateFunction;
import it.unimi.dsi.fastutil.doubles.DoubleArrayList;
import it.unimi.dsi.fastutil.doubles.DoubleList;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class CategoricalJoinAggregation {

  public static List<Aggregation> leftJoinAggregate(
      ColumnPair columnA, ColumnPair columnB, List<AggregateFunction> functions) {

    // create index for primary key in column B
    Map<String, DoubleArrayList> indexB = JoinAggregation.createKeyIndex(columnB);

    List<Aggregation> results = new ArrayList<>(functions.size());

    for (int fnIdx = 0; fnIdx < functions.size(); fnIdx++) {
      final AggregateFunction fn = functions.get(fnIdx);

      // numeric values for column A
      DoubleList joinValuesA = new DoubleArrayList(columnA.keyValues.size());

      // numeric values for each aggregation of column B
      DoubleList joinValuesB = new DoubleArrayList();

      // compute aggregation vectors of joined values for each join key
      for (int i = 0; i < columnA.keyValues.size(); i++) {
        String keyA = columnA.keyValues.get(i);
        final double valueA = columnA.columnValues[i];
        final DoubleArrayList rowsB = indexB.get(keyA);
        if (rowsB == null || rowsB.isEmpty()) {

        } else if (rowsB.size() == 1) {
          // 1:1 mapping, we use the single value.
          joinValuesA.add(valueA);
          joinValuesB.add(rowsB.getDouble(0));
        } else if (rowsB.size() == 1) {
          // 1:n mapping, we aggregate joined values to a single value.
          joinValuesA.add(valueA);
          joinValuesB.add(fn.aggregate(rowsB));
        }
      }
      results.add(new Aggregation(joinValuesA.toDoubleArray(), joinValuesB.toDoubleArray(), fn));
    }

    return results;
  }

  public static class Aggregation {
    public final double[] valuesA;
    public final double[] valuesB;
    public AggregateFunction aggregate;

    public Aggregation(double[] valuesA, double[] valuesB, AggregateFunction aggregate) {
      this.valuesA = valuesA;
      this.valuesB = valuesB;
      this.aggregate = aggregate;
    }
  }
}
