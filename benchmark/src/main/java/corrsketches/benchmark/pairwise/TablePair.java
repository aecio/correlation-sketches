package corrsketches.benchmark.pairwise;

import corrsketches.benchmark.ColumnPair;

public class TablePair {
  private ColumnPair x;
  private ColumnPair y;

  public TablePair(ColumnPair x, ColumnPair y) {
    this.x = x;
    this.y = y;
  }

  public ColumnPair getX() {
    return x;
  }

  public ColumnPair getY() {
    return y;
  }
}
