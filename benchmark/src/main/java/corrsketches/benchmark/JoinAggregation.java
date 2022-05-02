package corrsketches.benchmark;

import corrsketches.aggregations.AggregateFunction;
import it.unimi.dsi.fastutil.doubles.DoubleArrayList;
import it.unimi.dsi.fastutil.doubles.DoubleList;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

public class JoinAggregation {

  public static List<ColumnPair> aggregateColumnPair(
      ColumnPair cp, List<AggregateFunction> functions) {

    final Map<String, DoubleArrayList> index = createKeyIndex(cp);

    int i;
    double[][] aggregatedValues = new double[functions.size()][];
    for (i = 0; i < functions.size(); i++) {
      aggregatedValues[i] = new double[index.size()];
    }

    i = 0;
    String[] aggregatedKeys = new String[index.size()];
    for (Entry<String, DoubleArrayList> entry : index.entrySet()) {
      aggregatedKeys[i] = entry.getKey();
      for (int f = 0, functionsSize = functions.size(); f < functionsSize; f++) {
        final AggregateFunction fn = functions.get(f);
        aggregatedValues[f][i] = fn.aggregate(entry.getValue());
      }
      i++;
    }
    List<ColumnPair> results = new ArrayList<>();
    for (i = 0; i < functions.size(); i++) {
      results.add(
          new ColumnPair(
              cp.datasetId,
              cp.keyName,
              Arrays.asList(aggregatedKeys),
              cp.columnName,
              aggregatedValues[i]));
    }
    return results;
  }

  public static List<NumericJoinAggregation> numericJoinAggregate(
      ColumnPair columnA, ColumnPair columnB, List<AggregateFunction> functions) {

    // TODO: Aggregate left-side of the table?
    // FIXME: aggregate using given functions

    // create index for primary key in column B
    Map<String, DoubleArrayList> indexB = createKeyIndex(columnB);

    List<ColumnPair> aggregationsColumnA = aggregateColumnPair(columnA, functions);

    List<NumericJoinAggregation> results = new ArrayList<>(functions.size());

    for (int fnIdx = 0; fnIdx < functions.size(); fnIdx++) {
      final AggregateFunction fn = functions.get(fnIdx);

      ColumnPair aggregatedColumnA = aggregationsColumnA.get(fnIdx);

      // numeric values for column A
      DoubleList joinValuesA = new DoubleArrayList(aggregatedColumnA.keyValues.size());

      // numeric values for each aggregation of column B
      DoubleList joinValuesB = new DoubleArrayList();

      // compute aggregation vectors of joined values for each join key
      for (int i = 0; i < aggregatedColumnA.keyValues.size(); i++) {
        String keyA = aggregatedColumnA.keyValues.get(i);
        final double valueA = aggregatedColumnA.columnValues[i];
        final DoubleArrayList rowsB = indexB.get(keyA);
        if (rowsB != null && !rowsB.isEmpty()) {
          // 1:n mapping, we aggregate joined values to a single value.
          joinValuesA.add(valueA);
          joinValuesB.add(fn.aggregate(rowsB));
        }
      }
      results.add(
          new NumericJoinAggregation(joinValuesA.toDoubleArray(), joinValuesB.toDoubleArray(), fn));
    }

    return results;
  }

  protected static Map<String, DoubleArrayList> createKeyIndex(ColumnPair column) {
    Map<String, DoubleArrayList> index = new HashMap<>();
    for (int i = 0; i < column.keyValues.size(); i++) {
      DoubleArrayList doubles = index.get(column.keyValues.get(i));
      if (doubles == null) {
        doubles = new DoubleArrayList();
        index.put(column.keyValues.get(i), doubles);
      }
      doubles.add(column.columnValues[i]);
    }
    return index;
  }

  public static class NumericJoinAggregation {
    public final double[] valuesA;
    public final double[] valuesB;
    public AggregateFunction aggregate;

    public NumericJoinAggregation(double[] valuesA, double[] valuesB, AggregateFunction aggregate) {
      this.valuesA = valuesA;
      this.valuesB = valuesB;
      this.aggregate = aggregate;
    }
  }
}
