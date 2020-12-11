package corrsketches.benchmark;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.primitives.Doubles;
import corrsketches.aggregations.AggregateFunction;
import it.unimi.dsi.fastutil.doubles.DoubleArrayList;
import it.unimi.dsi.fastutil.doubles.DoubleList;
import java.util.List;

public class JoinAggregation {

  public static NumericJoinAggregation numericJoinAggregate(
      ColumnPair columnA, ColumnPair columnB, List<AggregateFunction> functions) {

    // create index for primary key in column B
    ArrayListMultimap<String, Double> columnMapB = ArrayListMultimap.create();
    for (int i = 0; i < columnB.keyValues.size(); i++) {
      columnMapB.put(columnB.keyValues.get(i), columnB.columnValues[i]);
    }

    // numeric values for column A
    DoubleList joinValuesA = new DoubleArrayList(columnA.keyValues.size());

    // numeric values for each aggregation of column B
    DoubleList[] joinValuesB = new DoubleArrayList[functions.size()];
    for (int i = 0; i < joinValuesB.length; i++) {
      joinValuesB[i] = new DoubleArrayList(columnA.keyValues.size());
    }

    // compute aggregation vectors of joined values for each join key
    for (int i = 0; i < columnA.keyValues.size(); i++) {
      String keyA = columnA.keyValues.get(i);
      double valueA = columnA.columnValues[i];

      List<Double> rowsB = columnMapB.get(keyA);

      if (rowsB == null || rowsB.isEmpty()) {
        // 1:0 mapping: we can't use null for correlation, so ignore row value.
        continue;
      } else if (rowsB.size() == 1) {
        // 1:1 mapping: there is nothing to aggregate, use the single value as is.
        joinValuesA.add(valueA);
        final double unboxedValue = rowsB.get(0);
        for (int fnIdx = 0; fnIdx < functions.size(); fnIdx++) {
          final AggregateFunction fn = functions.get(fnIdx);
          joinValuesB[fnIdx].add(fn.get().first(unboxedValue));
        }
      } else {
        // 1:n mapping, we aggregate joined values to a single value.
        joinValuesA.add(valueA);
        final double[] unboxedValues = Doubles.toArray(rowsB);
        for (int fnIdx = 0; fnIdx < functions.size(); fnIdx++) {
          final AggregateFunction fn = functions.get(fnIdx);
          final double aggregate = fn.aggregate(unboxedValues);
          joinValuesB[fnIdx].add(aggregate);
        }
      }
    }

    return new NumericJoinAggregation(joinValuesA, joinValuesB);
  }

  static class NumericJoinAggregation {
    public final DoubleList valuesA;
    public final DoubleList[] valuesB;

    public NumericJoinAggregation(DoubleList valuesA, DoubleList[] valuesB) {
      this.valuesA = valuesA;
      this.valuesB = valuesB;
    }
  }
}
