package corrsketches.benchmark;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import corrsketches.aggregations.AggregateFunction;
import corrsketches.benchmark.pairwise.ColumnCombination;
import corrsketches.benchmark.params.SketchParams;
import java.util.List;

public interface Benchmark {

  String csvHeader();

  List<String> run(
      ColumnCombination combination,
      List<SketchParams> sketchParams,
      List<AggregateFunction> leftAggregations,
      List<AggregateFunction> rightAggregations);

  abstract class BaseBenchmark<T> implements Benchmark {

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
  }
}
