package corrsketches.benchmark;

import it.unimi.dsi.fastutil.doubles.DoubleArrayList;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import tech.tablesaw.api.CategoricalColumn;
import tech.tablesaw.api.ColumnType;
import tech.tablesaw.api.NumericColumn;
import tech.tablesaw.api.Table;
import tech.tablesaw.io.csv.CsvReadOptions;
import tech.tablesaw.io.csv.CsvReadOptions.Builder;

public class Tables {

  public static List<String> findAllCSVs(String basePath) throws IOException {

    List<String> allFiles =
        Files.walk(Paths.get(basePath))
            .filter(p -> p.toString().endsWith(".csv"))
            .filter(Files::isRegularFile)
            .map(p -> p.toString())
            .collect(Collectors.toList());

    return allFiles;
  }

  public static Iterator<ColumnPair> readColumnPairs(String datasetFilePath, int minRows) {
    try {
      Table table = readTable(CsvReadOptions.builderFromFile(datasetFilePath));
      return readColumnPairs(Paths.get(datasetFilePath).getFileName().toString(), table, minRows);
    } catch (Exception e) {
      System.out.println("\nFailed to read dataset from file: " + datasetFilePath);
      e.printStackTrace(System.out);
      return Collections.emptyIterator();
    }
  }

  public static Iterator<ColumnPair> readColumnPairs(String datasetName, Table df, int minRows) {
    System.out.println("\nDataset: " + datasetName);

    System.out.printf("Row count: %d \n", df.rowCount());

    List<CategoricalColumn<String>> categoricalColumns = getStringColumns(df);
    System.out.println("Categorical columns: " + categoricalColumns.size());

    List<NumericColumn<?>> numericColumns = df.numericColumns();
    System.out.println("Numeric columns: " + numericColumns.size());

    if (df.rowCount() < minRows) {
      System.out.println("Column pairs: 0");
      return Collections.emptyIterator();
    }

    // Create a list of all column pairs
    List<ColumnEntry> pairs = new ArrayList<>();
    for (CategoricalColumn<?> key : categoricalColumns) {
      for (NumericColumn<?> column : numericColumns) {
        pairs.add(new ColumnEntry(key, column));
      }
    }
    System.out.println("Column pairs: " + pairs.size());
    if (pairs.isEmpty()) {
      return Collections.emptyIterator();
    }

    // Create a "lazy" iterator that creates one ColumnPair at a time to avoid overloading memory
    // with many large column pairs.
    Iterator<ColumnEntry> it = pairs.iterator();
    return new Iterator<ColumnPair>() {
      ColumnEntry nextPair = it.next();

      @Override
      public boolean hasNext() {
        return nextPair != null;
      }

      @Override
      public ColumnPair next() {
        ColumnEntry tmp = nextPair;
        nextPair = it.hasNext() ? it.next() : null;
        return Tables.createColumnPair(datasetName, tmp.key, tmp.column);
      }
    };
  }

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

  private static Table readTable(Builder csvReadOptionsBuilder) throws IOException {
    Table table =
        Table.read()
            .csv(
                csvReadOptionsBuilder
                    .sample(true)
                    .sampleSize(5_000_000)
                    .maxCharsPerColumn(10_000)
                    .missingValueIndicator("-"));
    return table;
  }

  public static List<Set<String>> readAllKeyColumns(String dataset) throws IOException {
    Table df = readTable(CsvReadOptions.builderFromFile(dataset));
    List<CategoricalColumn<String>> categoricalColumns = getStringColumns(df);
    List<Set<String>> allColumns = new ArrayList<>();
    for (CategoricalColumn<String> column : categoricalColumns) {
      Set<String> keySet = new HashSet<>();
      Iterator<String> it = column.iterator();
      while (it.hasNext()) {
        keySet.add(it.next());
      }
      allColumns.add(keySet);
    }
    return allColumns;
  }

  private static List<CategoricalColumn<String>> getStringColumns(Table df) {
    return df.columns()
        .stream()
        .filter(e -> e.type() == ColumnType.STRING || e.type() == ColumnType.TEXT)
        .map(e -> (CategoricalColumn<String>) e)
        .collect(Collectors.toList());
  }

  static class ColumnEntry {

    CategoricalColumn<?> key;
    NumericColumn<?> column;

    ColumnEntry(CategoricalColumn<?> key, NumericColumn<?> column) {
      this.key = key;
      this.column = column;
    }
  }
}
