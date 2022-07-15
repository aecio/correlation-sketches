package corrsketches.correlation;

import corrsketches.correlation.MutualInformation.MI;

public class MutualInformationMixed implements Correlation {

  public static final MutualInformationMixed INSTANCE = new MutualInformationMixed();

  @Override
  public Estimate ofCategorical(int[] x, int[] y) {
    return MutualInformation.ofCategorical(x, y);
  }

  @Override
  public MI ofCategoricalNumerical(int[] x, double[] y) {
    // TODO: How to compute the entropy of x an y in this case? Shall we use Eq. 4 from the KSG
    //   estimator paper which uses kNN with k=1? (see: Kraskov, Alexander, Harald Stögbauer, and
    //   Peter Grassberger. "Estimating mutual information." Physical review E 69, no. 6 (2004):
    //   066138.) Alternatively, we could use the estimators used in scipy library. From these
    //   Ebrahimi's estimator seems to be the best one. (see:
    //   https://link.springer.com/article/10.1007/s40745-015-0045-9)
    return new MI(MutualInformationDC.mi(x, y), x.length);
  }

  @Override
  public MI ofNumericalCategorical(double[] x, int[] y) {
    // TODO: Same as in ofCategoricalNumerical() method
    return new MI(MutualInformationDC.mi(y, x), x.length);
  }
}
