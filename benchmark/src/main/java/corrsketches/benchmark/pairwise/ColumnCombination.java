package corrsketches.benchmark.pairwise;

import corrsketches.benchmark.ColumnPair;

public interface ColumnCombination {
  ColumnPair getX();

  ColumnPair getY();
}
