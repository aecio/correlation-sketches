package corrsketches.sampling;

import it.unimi.dsi.fastutil.doubles.DoubleList;

public interface DoubleSampler {

  void sample(double item);

  DoubleList getSamples();
}
