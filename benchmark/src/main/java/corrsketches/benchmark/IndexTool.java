package corrsketches.benchmark;

import corrsketches.ColumnType;
import corrsketches.CorrelationSketch;
import corrsketches.CorrelationSketch.Builder;
import corrsketches.SketchType;
import corrsketches.SketchType.GKMVOptions;
import corrsketches.SketchType.KMVOptions;
import corrsketches.SketchType.SketchOptions;
import corrsketches.aggregations.AggregateFunction;
import corrsketches.benchmark.index.Hit;
import corrsketches.benchmark.index.QCRSketchIndex;
import corrsketches.benchmark.index.SketchIndex;
import corrsketches.benchmark.index.SortBy;
import corrsketches.correlation.CorrelationType;
import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(
    name = IndexTool.JOB_NAME,
    description = "Creates a Lucene index for a collection of tables")
public class IndexTool {

  public static final String JOB_NAME = "IndexTool";

  @Option(
      names = "--input-path",
      required = true,
      description = "Folder containing multiple tables in CSV or Parquet format")
  String inputPath;

  @Option(names = "--output-path", required = true, description = "Output path for results file")
  String outputPath;

  @Option(names = "--params", required = true, description = "Benchmark parameters")
  String params;

  @Option(
      names = "--aggregate",
      description = "The aggregate functions to be used by correlation sketches")
  AggregateFunction aggregate = AggregateFunction.FIRST;

  @Option(
      names = "--query-pair",
      required = false,
      description = "Query pair in format key_name:value_name")
  String queryPair;

  @Option(
      names = "--query-file-path",
      required = false,
      description = "Path to the table file containing the query pair")
  String queryFilePath;

  @Option(
      names = "--min-rows",
      required = false,
      description = "Minimum rows in a table required to index it")
  int minRows = 1;

  @Option(
      names = "--column-types",
      required = false,
      description = "Column types (NUMERICAL, CATEGORICAL)")
  ColumnType[] columnTypes =
      new ColumnType[] {
        ColumnType.NUMERICAL,
        // ColumnType.CATEGORICAL
      };

  @Option(
      names = "--top-k",
      required = false,
      description = "Number of top k correlations to output")
  int topK = 1000;

  @Option(
      names = "--estimator",
      required = false,
      description = "The correlation estimator to be used")
  CorrelationType estimator = CorrelationType.PEARSONS;

  public static void main(String[] args) {
    System.exit(new CommandLine(new IndexTool()).execute(args));
  }

  @Command(name = "buildIndex")
  public void buildIndex() throws Exception {
    var allTables = Tables.findAllTablesRelative(inputPath);
    var indexPath = Paths.get(outputPath, "index").toString();
    var index = createIndex(this.params, indexPath);
    buildIndex(inputPath, allTables, minRows, columnTypes, index);
    index.close();
    System.out.println("Done.");
  }

  public SketchIndex createIndex(String param, String indexPath) throws IOException {
    if (Files.exists(Paths.get(indexPath))) {
      System.out.printf("WARN: Index directory already exits: [%s].\n", indexPath);
    }
    final BenchmarkParams benchmarkParams = BenchmarkParams.parseValue(param);
    return openSketchIndex(indexPath, benchmarkParams, false);
  }

  private static void buildIndex(
      String basePath,
      List<String> allTables,
      int minRows,
      ColumnType[] columnTypes,
      SketchIndex index)
      throws IOException {
    System.out.println("Indexing all columns...");
    int i = 1;
    int columPairCount = 0;
    System.out.printf("Total tables: %d\n", allTables.size());
    for (var tablePath : allTables) {
      final double percent = i / (double) allTables.size() * 100;
      System.out.printf("table: %s\n", tablePath);
      String filepath = Paths.get(basePath, tablePath).toString();
      Iterator<ColumnPair> columnPairs =
          Tables.readColumnPairs(filepath, minRows, Set.of(columnTypes));
      if (!columnPairs.hasNext()) {
        continue;
      }
      while (columnPairs.hasNext()) {
        System.out.printf(
            "[%.2f%%] table %d out of %d. column pair: %d\n",
            percent, i, allTables.size(), columPairCount);
        ColumnPair cp = columnPairs.next();
        String id = String.format("%s/%s:%s", cp.datasetId, cp.keyName, cp.columnName);
        index.index(id, cp);
        columPairCount++;
      }
      i++;
    }
  }

  @Command(name = "queryIndex")
  public void queryIndex() throws Exception {
    if (queryPair == null || queryPair.isEmpty()) {
      System.out.println("The argument --query-pair is required for the queryIndex command.\n");
      System.exit(1);
    }
    var indexPath = Paths.get(inputPath, "index").toString();
    final BenchmarkParams benchmarkParams = BenchmarkParams.parseValue(this.params);
    var index = openSketchIndex(indexPath, benchmarkParams, true);

    var table = Tables.readTable(queryFilePath);
    String key = queryPair.split(":")[0];
    String value = queryPair.split(":")[1];
    var queryCP =
        Tables.createColumnPair(queryFilePath, table.categoricalColumn(key), table.column(value));

    List<Hit> hits = index.search(queryCP, topK);

    if (outputPath != null && !outputPath.isEmpty()) {
      Path path = Paths.get(outputPath, "query-" + queryPair + "-" + estimator + ".csv");
      System.out.println("Writing output to file: " + path.toAbsolutePath());
      PrintStream outputFile = new PrintStream(new FileOutputStream(path.toFile()));
      writeToStream(hits, outputFile);
      outputFile.close();
    } else {
      writeToStream(hits, System.out);
    }

    index.close();
    System.out.println("Done.");
  }

  private static void writeToStream(List<Hit> hits, PrintStream out) {
    out.print("hit,corr,containment,score,rerank_score\n");
    for (Hit hit : hits) {
      out.printf(
          "%s, %.6f, %.6f, %.6f, %.6f\n",
          hit.id, hit.correlation(), hit.joinability(), hit.score, hit.rerankScore);
    }
  }

  private SketchIndex openSketchIndex(String outputPath, BenchmarkParams params, boolean readonly)
      throws IOException {

    SketchType sketchType = params.sketchOptions.type;
    Builder builder = CorrelationSketch.builder();
    builder.aggregateFunction(aggregate);
    builder.estimator(estimator);
    switch (params.sketchOptions.type) {
      case KMV:
        builder.sketchType(sketchType, ((KMVOptions) params.sketchOptions).k);
        break;
      case GKMV:
        builder.sketchType(sketchType, ((GKMVOptions) params.sketchOptions).t);
        break;
      default:
        throw new IllegalArgumentException("Unsupported sketch type: " + params.sketchOptions.type);
    }

    String indexPath = indexPath(outputPath, params.indexType, params.sketchOptions);
    IndexType indexType = params.indexType;
    try {
      switch (indexType) {
        case STD:
          return new SketchIndex(indexPath, builder, params.sortBy, readonly);
        case QCR:
          return new QCRSketchIndex(indexPath, builder, params.sortBy, readonly);
        default:
          throw new IllegalArgumentException("Undefined index type: " + indexType);
      }
    } finally {
      System.out.printf("Opened index of type (%s) at: %s\n", indexType, indexPath);
    }
  }

  private static String indexPath(
      String outputPath, IndexType indexType, SketchOptions sketchOptions) {
    return Paths.get(outputPath, "indexes", indexType.toString() + ":" + sketchOptions.name())
        .toString();
  }

  public static class BenchmarkParams {

    public final String params;
    public final IndexType indexType;
    public final SortBy sortBy;
    public final int topK;
    public final SketchOptions sketchOptions;

    public BenchmarkParams(
        String params, IndexType indexType, SortBy sortBy, int topK, SketchOptions sketchOptions) {
      this.params = params;
      this.indexType = indexType;
      this.sortBy = sortBy;
      this.topK = topK;
      this.sketchOptions = sketchOptions;
    }

    public static List<BenchmarkParams> parse(String params) {
      String[] values = params.split(",");
      List<BenchmarkParams> result = new ArrayList<>();
      for (String value : values) {
        result.add(parseValue(value.trim()));
      }
      if (result.isEmpty()) {
        throw new IllegalArgumentException(
            String.format("[%s] does not have any valid sketch parameters", params));
      }
      return result;
    }

    private static BenchmarkParams parseValue(String params) {
      String[] values = params.split(":");
      final int argc = 5;
      if (values.length == argc) {
        int i = 0;
        IndexType indexType = IndexType.valueOf(values[i].trim());
        i++;
        SortBy sortBy = SortBy.valueOf(values[i].trim());
        i++;
        int topK = Integer.parseInt(values[i].trim());
        i++;
        SketchType sketchType = SketchType.valueOf(values[i].trim());
        i++;
        SketchOptions options = SketchType.parseOptions(sketchType, values[i]);
        return new BenchmarkParams(params, indexType, sortBy, topK, options);
      }
      throw new IllegalArgumentException(
          String.format("[%s] is not a valid parameter. Must have %d parameters", params, argc));
    }

    @Override
    public String toString() {
      return params;
    }
  }

  public enum IndexType {
    QCR,
    STD,
  }
}
