package corrsketches.correlation;

import corrsketches.correlation.MutualInformation.MI;
import corrsketches.statistics.Stats;

public class MutualInformationBinning implements Correlation {

  public static final MutualInformationBinning INSTANCE = new MutualInformationBinning();

  @Override
  public MI ofCategorical(int[] x, int[] y) {
    return MutualInformation.ofCategorical(x, y);
  }

  @Override
  public MI ofCategoricalNumerical(int[] x, double[] y) {
    return MutualInformation.ofCategorical(x, Stats.binEqualWidth(y));
  }

  @Override
  public MI ofNumericalCategorical(double[] x, int[] y) {
    return MutualInformation.ofCategorical(Stats.binEqualWidth(x), y);
  }

  @Override
  public MI ofNumerical(double[] x, double[] y) {
    return MutualInformation.ofCategorical(Stats.binEqualWidth(x), Stats.binEqualWidth(y));
  }
}
