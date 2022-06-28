package corrsketches.benchmark;

import corrsketches.ColumnType;
import java.util.List;
import java.util.Objects;

public class ColumnPair {

  public String datasetId;
  public String keyName;
  public List<String> keyValues;
  public String columnName;
  public ColumnType columnValueType;
  public double[] columnValues;

  public ColumnPair() {}

  public ColumnPair(
      String datasetId,
      String keyName,
      List<String> keyValues,
      String columnName,
      ColumnType valueType,
      double[] columnValues) {
    this.datasetId = datasetId;
    this.keyName = keyName;
    this.keyValues = keyValues;
    this.columnName = columnName;
    this.columnValueType = valueType;
    this.columnValues = columnValues;
  }

  @Override
  public String toString() {
    return "ColumnPair{"
        + "datasetId='"
        + datasetId
        + '\''
        + ", keyName='"
        + keyName
        + '\''
        + ", columnName='"
        + columnName
        + '\''
        + '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    ColumnPair that = (ColumnPair) o;
    return Objects.equals(datasetId, that.datasetId)
        && Objects.equals(keyName, that.keyName)
        && Objects.equals(columnName, that.columnName)
        && columnValueType == that.columnValueType;
  }

  @Override
  public int hashCode() {
    return Objects.hash(datasetId, keyName, columnName, columnValueType);
  }

  public String id() {
    return String.valueOf(hashCode());
  }
}
