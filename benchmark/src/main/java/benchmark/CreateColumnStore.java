package benchmark;

import com.github.rvesse.airline.annotations.Command;
import com.github.rvesse.airline.annotations.Option;
import com.github.rvesse.airline.annotations.restrictions.Required;
import com.google.common.base.Preconditions;
import hashtabledb.BytesBytesHashtable;
import hashtabledb.DBType;
import hashtabledb.Kryos;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import utils.CliTool;

@Command(
    name = CreateColumnStore.JOB_NAME,
    description = "Creates a column index to be used by the sketching benchmark")
public class CreateColumnStore extends CliTool implements Serializable {

  public static final String JOB_NAME = "CreateColumnStore";

  public static final Kryos<ColumnPair> KRYO = new Kryos(ColumnPair.class);
  public static final String COLUMNS_KEY = "columns";
  public static final String DBTYPE_KEY = "dbtype";

  @Required
  @Option(name = "--input-path", description = "Folder containing CSV files")
  String inputPath;

  @Required
  @Option(name = "--output-path", description = "Output path for key-value store with columns")
  String outputPath;

  @Option(name = "--min-rows", description = "Minimum number of rows to consider table")
  int minRows = 1;

  @Option(
      name = "--db-backend",
      description = "The type key-value store databaset: LEVELDB or ROCKSDB")
  DBType dbType = DBType.ROCKSDB;

  public static void main(String[] args) {
    CliTool.run(args, new CreateColumnStore());
  }

  @Override
  public void execute() throws Exception {
    Path db = Paths.get(outputPath);

    BytesBytesHashtable hashtable = new BytesBytesHashtable(dbType, db.toString());
    System.out.println("Created DB at " + db.toString());

    List<String> allCSVs = BenchmarkUtils.findAllCSVs(inputPath);
    System.out.println("> Found  " + allCSVs.size() + " CSV files at " + inputPath);

    FileWriter metadataFile = new FileWriter(getMetadataFilePath(outputPath).toFile());
    metadataFile.write(String.format("%s:%s\n", DBTYPE_KEY, dbType));

    System.out.println("\n> Writing columns to key-value DB...");

    Set<Set<String>> allColumns = new HashSet<>();
    for (String csv : allCSVs) {
      Set<ColumnPair> columnPairs = BenchmarkUtils.readColumnPairs(csv, minRows);
      Set<String> columnIds = new HashSet<>();
      for (ColumnPair cp : columnPairs) {
        String id = cp.id();
        hashtable.put(id.getBytes(), KRYO.serializeObject(cp));
        columnIds.add(id);
      }
      metadataFile.write(COLUMNS_KEY + ":");
      metadataFile.write(String.join(" ", columnIds));
      metadataFile.write("\n");
      allColumns.add(columnIds);
    }

    metadataFile.close();
    hashtable.close();

    System.out.println(getClass().getSimpleName() + " finished successfully.");

    System.out.println("Checking if can read written files...");
    ColumnStoreMetadata metadata = readMetadata(outputPath);
    Preconditions.checkArgument(metadata.columnSets.size() == allColumns.size());
    Preconditions.checkArgument(metadata.dbType == dbType);
    System.out.println("Check successfull.");
  }

  private static Path getMetadataFilePath(String outputPath) {
    return Paths.get(outputPath, "column-metadata.txt");
  }

  static ColumnStoreMetadata readMetadata(String outputPath) throws IOException {

    List<String> lines = Files.readAllLines(getMetadataFilePath(outputPath));

    Set<Set<String>> allColumns = new HashSet<>();
    DBType dbType = null;

    for (String line : lines) {
      if (line == null || line.isEmpty()) {
        continue;
      }
      if (line.startsWith(DBTYPE_KEY)) {
        String value = line.substring(DBTYPE_KEY.length() + 1);
        dbType = DBType.valueOf(value.trim());
      }
      if (line.startsWith(COLUMNS_KEY)) {
        String value = line.substring(COLUMNS_KEY.length() + 1);
        List<String> columns = Arrays.asList(value.split(" "));
        Set<String> columnIds = new HashSet<>(columns);
        allColumns.add(columnIds);
      }
    }

    if (dbType == null || allColumns.isEmpty()) {
      throw new IllegalStateException("Failed to read db type or columns from file: " + outputPath);
    }

    return new ColumnStoreMetadata(dbType, allColumns);
  }

  public static class ColumnStoreMetadata {
    final DBType dbType;
    final Set<Set<String>> columnSets;

    public ColumnStoreMetadata(DBType dbType, Set<Set<String>> columnSets) {
      this.columnSets = columnSets;
      this.dbType = dbType;
    }
  }
}
