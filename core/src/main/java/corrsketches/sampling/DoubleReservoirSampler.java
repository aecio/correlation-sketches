package corrsketches.sampling;

import it.unimi.dsi.fastutil.doubles.DoubleArrayList;
import it.unimi.dsi.fastutil.doubles.DoubleList;
import java.util.Random;

public class DoubleReservoirSampler implements DoubleSampler {

  private final Random random;
  private final int numSamples;
  private final DoubleList reservoir;
  int numItemsSeen = 0;

  public DoubleReservoirSampler(int numSamples) {
    this.numSamples = numSamples;
    this.reservoir = new DoubleArrayList(numSamples);
    this.random = new Random();
  }

  @Override
  public void update(double item) {
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
  public DoubleList values() {
    return reservoir;
  }
}
