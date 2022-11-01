package corrsketches.sampling;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class BernoulliSampler<T> implements Sampler<T> {

  private final Random rng;  // the random number generator used to sample items
  private final double prob; // the probability of including an item in the sample
  private final List<T> samples = new ArrayList<>();

  public BernoulliSampler(double prob) {
    this(prob, new Random(1237));
  }

  public BernoulliSampler(double prob, Random rng) {
    this.prob = prob;
    this.rng = rng;
  }

  @Override
  public void sample(T item) {
    if (rng.nextDouble() <= prob) {
      samples.add(item);
    }
  }

  @Override
  public List<T> getSamples() {
    return samples;
  }

  public List<T> samples() {
    return samples;
  }
}
