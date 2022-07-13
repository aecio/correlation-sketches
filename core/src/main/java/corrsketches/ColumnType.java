package corrsketches;

public enum ColumnType {
  NUMERICAL(1),
  CATEGORICAL(2);

  public final int intValue;

  ColumnType(int intValue) {
    this.intValue = intValue;
  }

  public static ColumnType valueOf(int intValue) {
    ColumnType type = null;
    for (ColumnType columnType : values()) {
      if (intValue == columnType.intValue) {
        type = columnType;
      }
    }
    if (type == null) {
      throw new IllegalArgumentException("No enum for the given intValue=[" + intValue + "].");
    }
    return type;
  }
}
