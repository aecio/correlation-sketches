package corrsketches.benchmark;

import com.google.common.base.Preconditions;
import corrsketches.SketchType;
import corrsketches.benchmark.index.SketchIndex;
import corrsketches.benchmark.utils.CliTool;
import edu.nyu.engineering.vida.kvdb4j.KryoIO;
import edu.nyu.engineering.vida.kvdb4j.api.KVDB;
import edu.nyu.engineering.vida.kvdb4j.api.StringObjectKVDB;
import edu.nyu.engineering.vida.kvdb4j.leveldb.LevelDBBackend;
import edu.nyu.engineering.vida.kvdb4j.rocksdb.RocksDBBackend;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.regex.Pattern;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(
    name = CreateColumnStore.JOB_NAME,
    description = "Creates a column index to be used by the sketching benchmark")
public class CreateColumnStore extends CliTool implements Serializable {

  public static final String JOB_NAME = "CreateColumnStore";

  public static final String COLUMNS_KEY = "columns";
  public static final String DBTYPE_KEY = "dbtype";

  @Option(names = "--input-path", required = true, description = "Folder containing CSV files")
  String inputPath;

  @Option(
      names = "--output-path",
      required = true,
      description = "Output path for key-value store with columns")
  String outputPath;

  @Option(names = "--min-rows", description = "Minimum number of rows to consider table")
  int minRows = 1;

  @Option(
      names = "--db-backend",
      description = "The type key-value store database: LEVELDB or ROCKSDB")
  DBType dbType = DBType.ROCKSDB;

  @Option(
      names = "--gen-synth-query-file",
      description = "If should generate query file for synthetic table corpus")
  boolean generateQueryFile = false;

  @Option(
      names = "--categorical",
      description = "If should use only categorical columns for value columns")
  Boolean categorical = false;

  public static void main(String[] args) {
    CliTool.run(args, new CreateColumnStore());
  }

  @Override
  public void execute() throws Exception {
    Path db = Paths.get(outputPath);

    ColumnStore store = createColumnStore(db, dbType);
    System.out.println("Created DB at " + db.toString());

    List<String> allCSVs = Tables.findAllTables(inputPath);
    System.out.println("> Found  " + allCSVs.size() + " CSV files at " + inputPath);

    FileWriter metadataFile = new FileWriter(getMetadataFilePath(outputPath).toFile());
    metadataFile.write(String.format("%s:%s\n", DBTYPE_KEY, dbType));

    FileWriter queryFile = null;
    Pattern rgx = null;
    if (generateQueryFile) {
      queryFile = new FileWriter(Paths.get(outputPath, "query-samples.txt").toFile());
      rgx = Pattern.compile("synthetic-bivariate_qid=[0-9]+[.]csv");
    }

    System.out.println("\n> Writing columns to key-value DB...");

    Set<Set<String>> allColumns = new HashSet<>();
    for (String csv : allCSVs) {
      Iterator<ColumnPair> columnPairs = Tables.readColumnPairs(csv, minRows, categorical);
      if (!columnPairs.hasNext()) {
        continue;
      }
      Set<String> columnIds = new HashSet<>();
      while (columnPairs.hasNext()) {
        ColumnPair cp = columnPairs.next();
        String id = cp.id();
        store.store(id, cp);
        columnIds.add(id);

        if (queryFile != null) {
          System.out.println(
              "datasetID: " + cp.datasetId + " matches: " + rgx.matcher(cp.datasetId).matches());
          if (rgx.matcher(cp.datasetId).matches()) {
            queryFile.write(cp.id());
            queryFile.write("\n");
          }
        }
      }
      metadataFile.write(COLUMNS_KEY + ":");
      metadataFile.write(String.join(" ", columnIds));
      metadataFile.write("\n");
      allColumns.add(columnIds);
    }

    metadataFile.close();
    store.close();
    if (queryFile != null) {
      queryFile.close();
    }

    System.out.println(getClass().getSimpleName() + " finished successfully.");

    System.out.println("Checking if can read written files...");
    ColumnStoreMetadata metadata = readMetadata(outputPath);
    Preconditions.checkArgument(metadata.columnSets.size() == allColumns.size());
    Preconditions.checkArgument(metadata.dbType == dbType);
    System.out.println("Check successful.");
  }

  private ColumnStore createColumnStore(Path db, DBType dbType) {
    if (dbType == DBType.LUCENE) {
      return new IndexColumnStore(db);
    } else {
      return new KVColumnStore(this.dbType, db);
    }
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
        if (!columnIds.isEmpty()) {
          allColumns.add(columnIds);
        }
      }
    }

    if (dbType == null || allColumns.isEmpty()) {
      throw new IllegalStateException("Failed to read db type or columns from file: " + outputPath);
    }

    return new ColumnStoreMetadata(dbType, allColumns);
  }

  public static QueryStats readQueries(String inputPath, ColumnStoreMetadata storeMetadata) {
    final Path queriesPath = Paths.get(inputPath, "query-samples.txt");
    System.out.println("Reading queries from file: " + queriesPath);
    try {
      if (!Files.exists(queriesPath)) {
        return null;
      }
      List<String> queries = Files.readAllLines(queriesPath);

      int totalColumns = 0;
      for (Set<String> columnSet : storeMetadata.columnSets) {
        totalColumns += columnSet.size();
      }

      System.out.println("Query examples:");
      for (int i = 0; i < 5 && i < queries.size(); i++) {
        System.out.printf(" [%d] %s", i, queries.get(i));
      }
      System.out.println();
      System.out.printf("Total columns: %d\tQueries selected: %d\n", totalColumns, queries.size());

      QueryStats stats = new QueryStats();
      stats.queries = new HashSet<>(queries);
      stats.totalColumns = totalColumns;
      return stats;

    } catch (IOException e) {
      throw new RuntimeException("Failed to read queries file: " + queriesPath, e);
    }
  }

  public static QueryStats selectQueriesRandomly(
      ColumnStoreMetadata storeMetadata, int sampleSize) {
    System.out.println("Selecting a random sample of columns as queries...");
    List<String> queries = new ArrayList<>();
    Random random = new Random(0);
    int seen = 0;
    for (Set<String> columnSet : storeMetadata.columnSets) {
      for (String column : columnSet) {
        if (queries.size() < sampleSize) {
          queries.add(column);
        } else {
          int index = random.nextInt(seen + 1);
          if (index < sampleSize) {
            queries.set(index, column);
          }
        }
        seen++;
      }
    }
    System.out.println("Query examples:");
    for (int i = 0; i < 5 && i < queries.size(); i++) {
      System.out.printf(" [%d] %s", i, queries.get(i));
    }
    System.out.println();
    System.out.printf("Total columns: %d\tQueries selected: %d\n", seen, queries.size());
    QueryStats stats = new QueryStats();
    stats.queries = new HashSet<>(queries);
    stats.totalColumns = seen;
    return stats;
  }

  interface ColumnStore {

    void store(String id, ColumnPair cp);

    void close() throws IOException;
  }

  public static class IndexColumnStore implements ColumnStore {

    SketchIndex index;

    public IndexColumnStore(Path path) {
      try {
        index = new SketchIndex(path.toString(), SketchType.KMV, 512);
      } catch (IOException e) {
        throw new RuntimeException("Failed to initialize index at " + path, e);
      }
    }

    @Override
    public void store(String id, ColumnPair cp) {
      try {
        index.index(id, cp);
      } catch (IOException e) {
        throw new RuntimeException("Failed to index column pair: " + id);
      }
    }

    @Override
    public void close() throws IOException {
      index.close();
    }
  }

  public static class KVColumnStore implements ColumnStore {

    final StringObjectKVDB<ColumnPair> db;

    public KVColumnStore(DBType dbType, Path db) {
      this.db = create(db.toString(), dbType, false);
    }

    public static StringObjectKVDB<ColumnPair> create(
        String inputPath, DBType dbType, boolean readonly) {
      final KVDB kvdb;
      if (dbType == DBType.ROCKSDB) {
        kvdb = RocksDBBackend.create(inputPath, readonly);
      } else if (dbType == DBType.LEVELDB) {
        kvdb = LevelDBBackend.create(inputPath);
      } else {
        throw new IllegalArgumentException("Unsupported database type: " + dbType);
      }
      return new StringObjectKVDB<>(kvdb, new KryoIO<>(ColumnPair.class));
    }

    @Override
    public void store(String id, ColumnPair cp) {
      db.put(id, cp);
    }

    @Override
    public void close() {
      db.close();
    }
  }

  static class QueryStats {

    int totalColumns;
    Set<String> queries;
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
