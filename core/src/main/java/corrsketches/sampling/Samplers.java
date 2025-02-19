package corrsketches.sampling;

public class Samplers {

  public static SamplerProvider reservoir(int maxSize) {
    return () -> new DoubleReservoirSampler(maxSize);
  }

  public static SamplerProvider bernoulli(double probability) {
    return () -> new BernoulliSampler(probability);
  }
}
