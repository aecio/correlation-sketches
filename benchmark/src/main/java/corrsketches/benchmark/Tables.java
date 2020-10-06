package corrsketches.benchmark;

import com.google.common.collect.ArrayListMultimap;
import corrsketches.correlation.PearsonCorrelation;
import corrsketches.correlation.QnCorrelation;
import corrsketches.correlation.RinCorrelation;
import corrsketches.correlation.SpearmanCorrelation;
import it.unimi.dsi.fastutil.doubles.DoubleArrayList;
import it.unimi.dsi.fastutil.doubles.DoubleList;
import java.util.ArrayList;
import java.util.List;
import tech.tablesaw.api.CategoricalColumn;
import tech.tablesaw.api.ColumnType;
import tech.tablesaw.api.NumericColumn;

public class Tables {

  public static ColumnPair createColumnPair(
      String dataset, CategoricalColumn<?> key, NumericColumn<?> column) {

    List<String> keyValues = new ArrayList<>();
    DoubleArrayList columnValues = new DoubleArrayList();

    if (column.type() == ColumnType.INTEGER) {
      Integer[] ints = (Integer[]) column.asObjectArray();
      for (int i = 0; i < ints.length; i++) {
        if (ints[i] != null) {
          columnValues.add(ints[i]);
          keyValues.add(key.getString(i));
        }
      }
    } else if (column.type() == ColumnType.LONG) {
      Long[] longs = (Long[]) column.asObjectArray();
      for (int i = 0; i < longs.length; i++) {
        if (longs[i] != null) {
          columnValues.add(longs[i]);
          keyValues.add(key.getString(i));
        }
      }
    } else if (column.type() == ColumnType.FLOAT) {
      Float[] floats = (Float[]) column.asObjectArray();
      for (int i = 0; i < floats.length; i++) {
        if (floats[i] != null) {
          columnValues.add(floats[i]);
          keyValues.add(key.getString(i));
        }
      }
    } else if (column.type() == ColumnType.DOUBLE) {
      Double[] doubles = (Double[]) column.asObjectArray();
      for (int i = 0; i < doubles.length; i++) {
        if (doubles[i] != null) {
          columnValues.add(doubles[i].doubleValue());
          keyValues.add(key.getString(i));
        }
      }
    } else {
      throw new IllegalArgumentException(
          String.format("Column of type %s can't be cast to double[]", column.type().toString()));
    }
    return new ColumnPair(
        dataset, key.name(), keyValues, column.name(), columnValues.toDoubleArray());
  }

  public static Correlations computePearsonAfterJoin(ColumnPair query, ColumnPair column) {

    ColumnPair columnA = query;
    ColumnPair columnB = column;

    // create index for primary key in column B
    ArrayListMultimap<String, Double> columnMapB = ArrayListMultimap.create();
    for (int i = 0; i < columnB.keyValues.size(); i++) {
      columnMapB.put(columnB.keyValues.get(i), columnB.columnValues[i]);
    }

    // loop over column B creating to new vectors index for primary key in column B
    DoubleList joinValuesA = new DoubleArrayList();
    DoubleList joinValuesB = new DoubleArrayList();
    for (int i = 0; i < columnA.keyValues.size(); i++) {
      String keyA = columnA.keyValues.get(i);
      double valueA = columnA.columnValues[i];
      List<Double> rowsB = columnMapB.get(keyA);
      if (rowsB != null && !rowsB.isEmpty()) {
        // TODO: We should properly handle cases where 1:N relationships happen.
        // We could could consider the correlation of valueA with an any aggregation function of the
        // list of values from B, e.g. mean, max, sum, count, etc.
        // Currently we are considering only the first seen value, and ignoring everything else,
        // similarly to the correlation sketch implementation.
        joinValuesA.add(valueA);
        joinValuesB.add(rowsB.get(0).doubleValue());
      }
    }

    // correlation is defined only for vectors of length at least two
    Correlations correlations = new Correlations();
    if (joinValuesA.size() < 2) {
      correlations.pearsons = Double.NaN;
      correlations.qn = Double.NaN;
      correlations.spearman = Double.NaN;
      correlations.rin = Double.NaN;
    } else {
      double[] joinedA = joinValuesA.toDoubleArray();
      double[] joinedB = joinValuesB.toDoubleArray();
      correlations.pearsons = PearsonCorrelation.coefficient(joinedA, joinedB);
      correlations.spearman = SpearmanCorrelation.coefficient(joinedA, joinedB);
      correlations.rin = RinCorrelation.coefficient(joinedA, joinedB);
      try {
        correlations.qn = QnCorrelation.correlation(joinedA, joinedB);
      } catch (Exception e) {
        correlations.qn = Double.NaN;
        System.out.printf(
            "Computation of Qn correlation failed for query id=%s [%s]] and column id=%s [%s]. "
                + "Array length after join is %d.\n",
            query.id(), query.toString(), column.id(), column.toString(), joinedA.length);
        System.out.printf("Error stack trace: %s\n", e.toString());
      }
    }

    return correlations;
  }

  public static ComputingTime timedComputePearsonAfterJoin(ColumnPair query, ColumnPair column) {
    ComputingTime time = new ComputingTime();

    ColumnPair columnA = query;
    ColumnPair columnB = column;
    long time0 = System.nanoTime();

    // create index for primary key in column B
    ArrayListMultimap<String, Double> columnMapB = ArrayListMultimap.create();
    for (int i = 0; i < columnB.keyValues.size(); i++) {
      columnMapB.put(columnB.keyValues.get(i), columnB.columnValues[i]);
    }

    // loop over column B creating to new vectors index for primary key in column B
    DoubleList joinValuesA = new DoubleArrayList();
    DoubleList joinValuesB = new DoubleArrayList();
    for (int i = 0; i < columnA.keyValues.size(); i++) {
      String keyA = columnA.keyValues.get(i);
      double valueA = columnA.columnValues[i];
      List<Double> rowsB = columnMapB.get(keyA);
      if (rowsB != null && !rowsB.isEmpty()) {
        // TODO: We should properly handle cases where 1:N relationships happen.
        // We could could consider the correlation of valueA with an any aggregation function of the
        // list of values from B, e.g. mean, max, sum, count, etc.
        // Currently we are considering only the first seen value, and ignoring everything else,
        // similarly to the correlation sketch implementation.
        joinValuesA.add(valueA);
        joinValuesB.add(rowsB.get(0).doubleValue());
      }
    }
    time.join = System.nanoTime() - time0;

    // correlation is defined only for vectors of length at least two
    Correlations correlations = new Correlations();
    if (joinValuesA.size() < 2) {
      correlations.pearsons = Double.NaN;
      correlations.qn = Double.NaN;
      correlations.spearman = Double.NaN;
      correlations.rin = Double.NaN;
    } else {
      double[] joinedA = joinValuesA.toDoubleArray();
      double[] joinedB = joinValuesB.toDoubleArray();

      time0 = System.nanoTime();
      correlations.spearman = SpearmanCorrelation.coefficient(joinedA, joinedB);
      time.spearmans = System.nanoTime() - time0;

      time0 = System.nanoTime();
      correlations.pearsons = PearsonCorrelation.coefficient(joinedA, joinedB);
      time.pearsons = System.nanoTime() - time0;

      time0 = System.nanoTime();
      correlations.rin = RinCorrelation.coefficient(joinedA, joinedB);
      time.rin = System.nanoTime() - time0;

      try {
        time0 = System.nanoTime();
        correlations.qn = QnCorrelation.correlation(joinedA, joinedB);
        time.qn = System.nanoTime() - time0;
      } catch (Exception e) {
        correlations.qn = Double.NaN;
        System.out.printf(
            "Computation of Qn correlation failed for query id=%s [%s]] and column id=%s [%s]. "
                + "Array length after join is %d.\n",
            query.id(), query.toString(), column.id(), column.toString(), joinedA.length);
        System.out.printf("Error stack trace: %s\n", e.toString());
      }
    }

    return time;
  }

  static class ComputingTime {
    public long join = -1;
    public long spearmans = -1;
    public long pearsons = -1;
    public long rin = -1;
    public long qn = -1;
  }

  static class Correlations {
    public double spearman;
    public double rin;
    double pearsons;
    double qn;
  }
}
