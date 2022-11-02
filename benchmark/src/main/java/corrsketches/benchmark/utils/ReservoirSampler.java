package corrsketches.benchmark.utils;

import com.google.common.collect.ImmutableList;
import corrsketches.sampling.Sampler;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * Perform sampling using reservoir sampling algorithm. If number of items provided is smaller than
 * the total number of desired samples, all items are stored. Otherwise, a random sample of size
 * numSamples is stored.
 */
public class ReservoirSampler<T> implements Sampler<T> {

  private final Random random;
  private final int numSamples;
  private final List<T> reservoir;
  int numItemsSeen = 0;

  public ReservoirSampler(int numSamples) {
    this.numSamples = numSamples;
    this.reservoir = new ArrayList<>(numSamples);
    this.random = new Random(0);
  }

  /**
   * Perform sampling using reservoir sampling algorithm. If number os combinations is smaller than
   * the total number of desired samples, all combinations are kept. Otherwise, a random sample of
   * size numSamples is returned.
   */
  @Override
  public void sample(T item) {
    if (reservoir.size() < numSamples) {
      // when the reservoir not full, just append
      reservoir.add(item);
    } else {
      // when it is full, randomly select a sample to replace
      int randomIndex = random.nextInt(numItemsSeen + 1);
      if (randomIndex < numSamples) {
        reservoir.set(randomIndex, item);
      }
    }
    numItemsSeen++;
  }

  @Override
  public List<T> getSamples() {
    return ImmutableList.copyOf(reservoir);
  }
}
