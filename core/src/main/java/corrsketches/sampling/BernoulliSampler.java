package corrsketches.sampling;

import it.unimi.dsi.fastutil.doubles.DoubleArrayList;
import it.unimi.dsi.fastutil.doubles.DoubleList;
import java.util.Random;

public class BernoulliSampler implements DoubleSampler {

  private final Random rng; // the random number generator used to sample items
  private final double prob; // the probability of including an item in the sample
  private final DoubleList samples = new DoubleArrayList();

  public BernoulliSampler(double prob) {
    this(prob, new Random(1237));
  }

  public BernoulliSampler(double prob, Random rng) {
    this.prob = prob;
    this.rng = rng;
  }

  @Override
  public void sample(double item) {
    if (rng.nextDouble() <= prob) {
      samples.add(item);
    }
  }

  @Override
  public DoubleList getSamples() {
    return samples;
  }
}
