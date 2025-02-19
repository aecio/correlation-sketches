package corrsketches.sampling;

import corrsketches.ColumnType;
import corrsketches.aggregations.RepeatedValueHandler;
import it.unimi.dsi.fastutil.doubles.DoubleList;

public interface DoubleSampler extends RepeatedValueHandler {

  @Override
  default void first(double value) {
    this.update(value);
  }

  @Override
  void update(double current);

  @Override
  default boolean isAggregator() {
    return false;
  }

  @Override
  default double aggregatedValue() {
    throw new UnsupportedOperationException("Sampler does not support value aggregation.");
  }

  @Override
  DoubleList values();

  @Override
  default ColumnType getOutputType(ColumnType columnValueType) {
    return columnValueType;
  }

  default boolean acceptsInputColumnType(ColumnType inputDataType) {
    return true;
  }
}
