package corrsketches.sampling;

import java.util.List;

public interface Sampler<T> {

  void sample(T item);

  List<T> getSamples();
}
