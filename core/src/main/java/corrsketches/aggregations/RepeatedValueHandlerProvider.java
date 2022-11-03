package corrsketches.aggregations;

/** Creates an instance of an object that implements the RepeatedValueProvider interface. */
public interface RepeatedValueHandlerProvider {

  RepeatedValueHandler create();
}
