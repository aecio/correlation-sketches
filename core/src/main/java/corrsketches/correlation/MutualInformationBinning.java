package corrsketches.correlation;

import corrsketches.statistics.Stats;

public class MutualInformationBinning implements Correlation<MIEstimate> {

  public static final MutualInformationBinning INSTANCE = new MutualInformationBinning();

  @Override
  public MIEstimate ofCategorical(int[] x, int[] y) {
    return MutualInformation.ofCategorical(x, y);
  }

  @Override
  public MIEstimate ofCategoricalNumerical(int[] x, double[] y) {
    return MutualInformation.ofCategorical(x, Stats.binEqualWidth(y));
  }

  @Override
  public MIEstimate ofNumericalCategorical(double[] x, int[] y) {
    return MutualInformation.ofCategorical(Stats.binEqualWidth(x), y);
  }

  @Override
  public MIEstimate ofNumerical(double[] x, double[] y) {
    return MutualInformation.ofCategorical(Stats.binEqualWidth(x), Stats.binEqualWidth(y));
  }
}
