package corrsketches.benchmark;

import corrsketches.Column;
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

      var joinStats = new JoinStats();

      // Join keys for column A
      List<String> joinKeysA = new ArrayList<>();

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
          joinStats.join_1to0++;
        } else {
          if (rowsB.size() == 1) {
            // 1:1 mapping
            joinStats.join_1to1++;
          } else {
            // 1:n mapping
            joinStats.join_1toN++;
          }
          joinKeysA.add(keyA);
          joinValuesA.add(valueA);
          // We need to aggregate even for 1:1 mappings, because some
          // aggregate functions may transform the original value (e.g., COUNT)
          joinValuesB.add(fn.aggregate(rowsB));
        }
      }
      results.add(
          new Aggregation(
              joinKeysA,
              Column.of(joinValuesA.toDoubleArray(), columnA.columnValueType),
              Column.of(
                  joinValuesB.toDoubleArray(), fn.get().getOutputType(columnB.columnValueType)),
              fn,
              joinStats));
    }

    return results;
  }

  static class JoinStats {
    public int join_1to0 = 0;
    public int join_1to1 = 0;
    public int join_1toN = 0;
  }

  public static class Aggregation {

    public final List<String> keys;
    public final Column a;
    public final Column b;
    public final AggregateFunction aggregate;
    public final JoinStats joinStats;

    public Aggregation(
        List<String> keys, Column a, Column b, AggregateFunction aggregate, JoinStats joinStats) {
      this.keys = keys;
      this.a = a;
      this.b = b;
      this.aggregate = aggregate;
      this.joinStats = joinStats;
    }
  }
}
