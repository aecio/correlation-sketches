package corrsketches.aggregations;

import corrsketches.ColumnType;
import it.unimi.dsi.fastutil.doubles.DoubleList;

public interface RepeatedValueHandler {

  void first(double value);

  void update(double current);

  boolean isAggregator();

  double aggregatedValue();

  DoubleList values();

  ColumnType getOutputType(ColumnType columnValueType);

  boolean acceptsInputColumnType(ColumnType inputDataType);
}
