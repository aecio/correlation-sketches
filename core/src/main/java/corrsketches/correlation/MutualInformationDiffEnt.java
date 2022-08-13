package corrsketches.correlation;

import corrsketches.correlation.MutualInformation.MI;
import corrsketches.statistics.DifferentialEntropy;
import corrsketches.statistics.Entropy;

public class MutualInformationDiffEnt implements Correlation<MI> {

  public static final MutualInformationDiffEnt INSTANCE = new MutualInformationDiffEnt();

  private final int k;

  public MutualInformationDiffEnt() {
    this(3);
  }

  public MutualInformationDiffEnt(int k) {
    this.k = k;
  }

  @Override
  public MI ofCategorical(int[] x, int[] y) {
    return MutualInformation.ofCategorical(x, y);
  }

  @Override
  public MI ofCategoricalNumerical(int[] x, double[] y) {
    // y = Stats.addRandomNoise(y);
    final double ex = Entropy.entropy(x);
    final double ey = DifferentialEntropy.entropy(y, k);
    final double mi = MutualInformationDC.mi(x, y, k);
    return new MI(mi, x.length, ex, ey);
  }

  @Override
  public MI ofNumericalCategorical(double[] x, int[] y) {
    // x = Stats.addRandomNoise(x);
    final double ex = DifferentialEntropy.entropy(x, k);
    final double ey = Entropy.entropy(y);
    final double mi = MutualInformationDC.mi(y, x, k);
    return new MI(mi, x.length, ex, ey);
  }

  @Override
  public MI ofNumerical(double[] x, double[] y) {
    final double ex = DifferentialEntropy.entropy(x, k);
    final double ey = DifferentialEntropy.entropy(y, k);
    final double mi = MutualInformationMixedKSG.mi(y, x, k);
    return new MI(mi, x.length, ex, ey);
  }
}
