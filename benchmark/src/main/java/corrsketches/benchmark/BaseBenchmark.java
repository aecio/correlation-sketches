package corrsketches.benchmark;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import corrsketches.benchmark.pairwise.ColumnCombination;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public abstract class BaseBenchmark<T> implements Benchmark {

  private final ObjectWriter csvWriter;
  private final ObjectWriter csvHeaderWriter;

  public BaseBenchmark(Class<T> clazz) {
    CsvMapper mapper = new CsvMapper();
    CsvSchema schema = mapper.schemaFor(clazz);
    csvWriter = mapper.writer(schema);
    csvHeaderWriter = mapper.writer(mapper.schemaFor(clazz).withHeader());
  }

  public String csvHeader() {
    try {
      return csvHeaderWriter.writeValueAsString(null).trim();
    } catch (JsonProcessingException e) {
      throw new RuntimeException("Failed to generate CSV header", e);
    }
  }

  public String toCsvLine(T result) {
    try {
      return csvWriter.writeValueAsString(result);
    } catch (JsonProcessingException e) {
      throw new IllegalArgumentException("Failed to serialize result to CSV line");
    }
  }

  public String toCsvLines(List<T> results) {
    if (results == null || results.isEmpty()) {
      return "";
    } else {
      StringBuilder builder = new StringBuilder();
      for (T result : results) {
        builder.append(toCsvLine(result));
      }
      return builder.toString();
    }
  }

  public static void runParallel(
      int totalTasks,
      int taskId,
      int cpuCores,
      List<? extends ColumnCombination> combinations,
      Benchmark bench,
      String outputPath,
      String filename)
      throws Exception {
    // Initialize CSV output file and start writing headers
    Files.createDirectories(Paths.get(outputPath));
    FileWriter resultsFile = new FileWriter(Paths.get(outputPath, filename).toString());
    resultsFile.write(bench.csvHeader() + "\n");

    // If necessary, filter combinations leaving only the ones that should be computed
    // by this task
    System.out.println("\n> Total number of column combinations: " + combinations.size());
    if (totalTasks > 1) {
      combinations =
          IntStream.range(0, combinations.size())
              .filter(i -> i % totalTasks == taskId)
              .mapToObj(combinations::get)
              .collect(Collectors.toList());
      System.out.println("Column combinations for this task: " + combinations.size());
    }

    final Stream<? extends ColumnCombination> stream = combinations.stream();
    final int total = combinations.size();
    final AtomicInteger processed = new AtomicInteger(0);
    final int cores = cpuCores > 0 ? cpuCores : Runtime.getRuntime().availableProcessors();

    System.out.println("\n> Running...");
    ForkJoinPool forkJoinPool = new ForkJoinPool(cores);
    Runnable task =
        () ->
            stream
                .parallel()
                .map(
                    (ColumnCombination combination) -> {
                      List<String> results = bench.computeResults(combination);
                      reportProgress(processed, total);
                      return toCSV(results);
                    })
                .forEach(writeCSV(resultsFile));
    forkJoinPool.submit(task).get();

    resultsFile.close();
  }

  static void reportProgress(AtomicInteger processed, int total) {
    int current = processed.incrementAndGet();
    if (current % 1000 == 0) {
      double percent = 100 * current / (double) total;
      synchronized (System.out) {
        System.out.printf("Progress: %.3f%%\n", percent);
      }
    }
  }

  protected static String toCSV(List<String> results) {
    if (results == null || results.isEmpty()) {
      return "";
    } else {
      StringBuilder builder = new StringBuilder();
      for (String result : results) {
        builder.append(result);
      }
      return builder.toString();
    }
  }

  protected static Consumer<String> writeCSV(FileWriter file) {
    return (String line) -> {
      if (!line.isEmpty()) {
        synchronized (file) {
          try {
            file.write(line);
            file.flush();
          } catch (IOException e) {
            throw new RuntimeException("Failed to write line to file: " + line);
          }
        }
      }
    };
  }
}
