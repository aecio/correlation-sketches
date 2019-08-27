package sketches.correlation.benchmark;

import java.util.List;
import java.util.Objects;

public class ColumnPair {

    protected String datasetId;
    protected String keyName;
    protected List<String> keyValues;
    protected String columnName;
    protected double[] columnValues;

    public ColumnPair(String datasetId, String keyName, List<String> keyValues,
            String columnName, double[] columnValues) {
        this.datasetId = datasetId;
        this.keyName = keyName;
        this.keyValues = keyValues;
        this.columnName = columnName;
        this.columnValues = columnValues;
    }

    @Override
    public String toString() {
        return "ColumnPair{" +
                "datasetId='" + datasetId + '\'' +
                ", keyName='" + keyName + '\'' +
                ", columnName='" + columnName + '\'' +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ColumnPair that = (ColumnPair) o;
        return Objects.equals(datasetId, that.datasetId) &&
                Objects.equals(keyName, that.keyName) &&
                Objects.equals(columnName, that.columnName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(datasetId, keyName, columnName);
    }

    public String id() {
        return String.valueOf(hashCode());
    }
}