package corrsketches.benchmark.pairwise;

public interface SyntheticColumnCombination extends ColumnCombination {

  float getCorrelation();

  String getKeyDistribution();
}
