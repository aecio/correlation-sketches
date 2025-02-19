package corrsketches.benchmark;

import corrsketches.benchmark.pairwise.ColumnCombination;
import java.util.List;

public interface Benchmark {

  String csvHeader();

  List<String> computeResults(ColumnCombination combination);
}
