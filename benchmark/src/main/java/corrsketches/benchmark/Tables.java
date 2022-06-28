package corrsketches.benchmark;

import corrsketches.ColumnType;
import corrsketches.util.Hashes;
import it.unimi.dsi.fastutil.doubles.DoubleArrayList;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import net.tlabs.tablesaw.parquet.TablesawParquetReadOptions;
import net.tlabs.tablesaw.parquet.TablesawParquetReader;
import tech.tablesaw.api.CategoricalColumn;
import tech.tablesaw.api.NumericColumn;
import tech.tablesaw.api.StringColumn;
import tech.tablesaw.api.Table;
import tech.tablesaw.columns.Column;
import tech.tablesaw.io.csv.CsvReadOptions;

public class Tables {

  public static List<String> findAllTables(String basePath) throws IOException {
    try (final var pathStream = Files.walk(Paths.get(basePath))) {
      return pathStream
          .filter(p -> p.toString().endsWith(".csv") || p.toString().endsWith(".parquet"))
          .filter(Files::isRegularFile)
          .map(Path::toString)
          .collect(Collectors.toList());
    }
  }

  public static Iterator<ColumnPair> readColumnPairs(
      String datasetFilePath, int minRows, ColumnType columnType) {
    try {
      Table table = readTable(datasetFilePath);
      String datasetName = Paths.get(datasetFilePath).getFileName().toString();
      return readColumnPairs(datasetName, table, minRows, columnType);
    } catch (Exception e) {
      System.out.println("\nFailed to read dataset from file: " + datasetFilePath);
      e.printStackTrace(System.out);
      return Collections.emptyIterator();
    }
  }

  public static Iterator<ColumnPair> readColumnPairs(
      String datasetName, Table df, int minRows, ColumnType valueColumnType) {
    System.out.println("\nDataset: " + datasetName);

    System.out.printf("Row count: %d \n", df.rowCount());

    List<CategoricalColumn<String>> joinKeyColumns = getStringColumns(df);
    System.out.println("Join key columns: " + joinKeyColumns.size());

    List<Column<?>> valueColumns;
    if (valueColumnType == ColumnType.CATEGORICAL) {
      valueColumns =
          df.columns().stream()
              .filter(
                  e ->
                      e.type() == tech.tablesaw.api.ColumnType.STRING
                          || e.type() == tech.tablesaw.api.ColumnType.TEXT)
              .map(e -> (CategoricalColumn<String>) e)
              .collect(Collectors.toList());
      System.out.println("String columns: " + valueColumns.size());
    } else if (valueColumnType == ColumnType.NUMERICAL) {
      valueColumns =
          df.columns().stream()
              .filter(e -> e instanceof NumericColumn<?>)
              .collect(Collectors.toList());
      System.out.println("Numerical columns: " + valueColumns.size());
    } else {
      throw new IllegalStateException("Invalid table column type." + valueColumnType.toString());
    }

    if (df.rowCount() < minRows) {
      System.out.println("Column pairs: 0");
      return Collections.emptyIterator();
    }

    // Create a list of all column pairs
    List<ColumnEntry> pairs = new ArrayList<>();
    for (CategoricalColumn<?> key : joinKeyColumns) {
      for (Column<?> column : valueColumns) {
        pairs.add(new ColumnEntry(key, column));
      }
    }
    System.out.println("Column pairs: " + pairs.size());
    if (pairs.isEmpty()) {
      return Collections.emptyIterator();
    }

    // Create a "lazy" iterator that creates one ColumnPair at a time to avoid overloading memory
    // with many large column pairs.
    return new ColumnPairIterator(datasetName, pairs);
  }

  public static ColumnPair createColumnPair(
      String dataset, CategoricalColumn<?> key, Column<?> column) {

    List<String> keyValues = new ArrayList<>();
    DoubleArrayList columnValues = new DoubleArrayList();
    ColumnType valueType;
    if (column.type() == tech.tablesaw.api.ColumnType.INTEGER) {
      valueType = ColumnType.NUMERICAL;
      Integer[] ints = (Integer[]) column.asObjectArray();
      for (int i = 0; i < ints.length; i++) {
        if (ints[i] != null) {
          columnValues.add(ints[i]);
          keyValues.add(key.getString(i));
        }
      }
    } else if (column.type() == tech.tablesaw.api.ColumnType.LONG) {
      valueType = ColumnType.NUMERICAL;
      Long[] longs = (Long[]) column.asObjectArray();
      for (int i = 0; i < longs.length; i++) {
        if (longs[i] != null) {
          columnValues.add(longs[i]);
          keyValues.add(key.getString(i));
        }
      }
    } else if (column.type() == tech.tablesaw.api.ColumnType.FLOAT) {
      valueType = ColumnType.NUMERICAL;
      Float[] floats = (Float[]) column.asObjectArray();
      for (int i = 0; i < floats.length; i++) {
        if (floats[i] != null) {
          columnValues.add(floats[i]);
          keyValues.add(key.getString(i));
        }
      }
    } else if (column.type() == tech.tablesaw.api.ColumnType.DOUBLE) {
      valueType = ColumnType.NUMERICAL;
      Double[] doubles = (Double[]) column.asObjectArray();
      for (int i = 0; i < doubles.length; i++) {
        if (doubles[i] != null) {
          columnValues.add(doubles[i].doubleValue());
          keyValues.add(key.getString(i));
        }
      }
    } else if (column.type() == tech.tablesaw.api.ColumnType.TEXT
        || column.type() == tech.tablesaw.api.ColumnType.STRING) {
      valueType = ColumnType.CATEGORICAL;
      StringColumn values = column.asStringColumn();
      for (int i = 0; i < values.size(); i++) {
        if (!values.isMissing(i)) {
          // we store the text hash (integer) as double variables
          columnValues.add(Hashes.murmur3_32(values.get(i)));
          keyValues.add(key.getString(i));
        }
      }
    } else {
      throw new IllegalArgumentException(
          String.format("Column of type %s can't be cast to double[]", column.type().toString()));
    }

    return new ColumnPair(
        dataset, key.name(), keyValues, column.name(), valueType, columnValues.toDoubleArray());
  }

  private static Table readTable(String datasetFilePath) throws IOException {

    if (datasetFilePath.endsWith("csv")) {
      // Read CSV files
      return Table.read()
          .csv(
              CsvReadOptions.builderFromFile(datasetFilePath)
                  .sample(true)
                  .sampleSize(5_000_000)
                  .maxCharsPerColumn(10_000)
                  .missingValueIndicator("-"));
    } else if (datasetFilePath.endsWith("parquet")) {
      // Read Parquet files
      return new TablesawParquetReader()
          .read(TablesawParquetReadOptions.builder(datasetFilePath).sample(true).build());
    } else {
      throw new IllegalArgumentException("Invalid file extension in file: " + datasetFilePath);
    }
  }

  public static List<Set<String>> readAllKeyColumns(String dataset) throws IOException {
    Table df = readTable(dataset);
    List<CategoricalColumn<String>> categoricalColumns = getStringColumns(df);
    List<Set<String>> allColumns = new ArrayList<>();
    for (CategoricalColumn<String> column : categoricalColumns) {
      Set<String> keySet = new HashSet<>();
      for (String s : column) {
        keySet.add(s);
      }
      allColumns.add(keySet);
    }
    return allColumns;
  }

  private static List<CategoricalColumn<String>> getStringColumns(Table df) {
    return df.columns().stream()
        .filter(
            e ->
                e.type() == tech.tablesaw.api.ColumnType.STRING
                    || e.type() == tech.tablesaw.api.ColumnType.TEXT)
        .map(e -> (CategoricalColumn<String>) e)
        .collect(Collectors.toList());
  }

  static class ColumnEntry {

    final CategoricalColumn<?> key;
    final Column<?> column;

    ColumnEntry(CategoricalColumn<?> key, Column<?> column) {
      this.key = key;
      this.column = column;
    }
  }

  public static class ColumnPairIterator implements Iterator<ColumnPair> {

    private final String datasetName;
    private final Iterator<ColumnEntry> it;
    private ColumnEntry nextPair;

    public ColumnPairIterator(String datasetName, List<ColumnEntry> pairs) {
      this.datasetName = datasetName;
      this.it = pairs.iterator();
      this.nextPair = this.it.next();
    }

    @Override
    public boolean hasNext() {
      return nextPair != null;
    }

    @Override
    public ColumnPair next() {
      ColumnEntry tmp = nextPair;
      this.nextPair = it.hasNext() ? it.next() : null;
      return Tables.createColumnPair(datasetName, tmp.key, tmp.column);
    }
  }
}
