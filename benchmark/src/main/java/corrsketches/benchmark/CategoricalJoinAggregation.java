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
        } else if (rowsB.size() == 1) {
          // 1:1 mapping, we use the single value.
          joinKeysA.add(keyA);
          joinValuesA.add(valueA);
          joinValuesB.add(rowsB.getDouble(0));
          joinStats.join_1to1++;
        } else {
          // 1:n mapping, we aggregate joined values to a single value.
          joinKeysA.add(keyA);
          joinValuesA.add(valueA);
          joinValuesB.add(fn.aggregate(rowsB));
          joinStats.join_1toN++;
        }
      }
      results.add(
          new Aggregation(
              joinKeysA, joinValuesA.toDoubleArray(), joinValuesB.toDoubleArray(), fn, joinStats));
    }

    return results;
  }

  static class JoinStats {
    public int join_1to0 = 0;
    public int join_1to1 = 0;
    public int join_1toN = 0;
  }

  public static class Aggregation {

    public List<String> keys;
    public final double[] valuesA;
    public final double[] valuesB;
    public AggregateFunction aggregate;
    public JoinStats joinStats;

    public Aggregation(
        List<String> keys,
        double[] valuesA,
        double[] valuesB,
        AggregateFunction aggregate,
        JoinStats joinStats) {
      this.keys = keys;
      this.valuesA = valuesA;
      this.valuesB = valuesB;
      this.aggregate = aggregate;
      this.joinStats = joinStats;
    }
  }
}
