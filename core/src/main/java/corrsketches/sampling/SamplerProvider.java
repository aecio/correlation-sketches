package corrsketches.sampling;

import corrsketches.aggregations.RepeatedValueHandler;
import corrsketches.aggregations.RepeatedValueHandlerProvider;

public interface SamplerProvider extends RepeatedValueHandlerProvider {

  default RepeatedValueHandler create() {
    return get();
  }

  DoubleSampler get();
}
