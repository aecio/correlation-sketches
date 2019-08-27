package sketches.correlation.benchmark;

import com.google.common.collect.ArrayListMultimap;
import it.unimi.dsi.fastutil.doubles.DoubleArrayList;
import it.unimi.dsi.fastutil.doubles.DoubleList;
import java.util.List;
import sketches.correlation.PearsonCorrelation;
import tech.tablesaw.api.ColumnType;
import tech.tablesaw.columns.Column;

public class Tables {

    public static double[] doubleArray(Column column) {
        double[] array = new double[column.size()];
        if (column.type() == ColumnType.INTEGER) {
            Integer[] ints = (Integer[]) column.asObjectArray();
            for (int i = 0; i < ints.length; i++) {
                if (ints[i] == null) {
                    throw new IllegalArgumentException("Cannot convert missing int value (null) to double type.");
                } else {
                    array[i] = ints[i];
                }
            }
        } else if(column.type() == ColumnType.DOUBLE) {
            Double[] doubles = (Double[]) column.asObjectArray();
            for (int i = 0; i < doubles.length; i++) {
                if (doubles[i] == null) {
                    array[i] = Double.NaN;
//                    throw new IllegalArgumentException("Cannot convert missing value (null) to double type.");
                } else {
                    array[i] = doubles[i];
                }
            }
        } else {
            throw new IllegalArgumentException(
                    String.format("Column of type %s can't be cast to double[]", column.type().toString())
            );
        }
        return array;
    }

    public static double computePearsonAfterJoin(ColumnPair query, ColumnPair column) {

        ColumnPair columnA = query;
        ColumnPair columnB = column;

        // create index for primary key in column B
        ArrayListMultimap<String, Double> columnMapB = ArrayListMultimap.create();
        for (int i = 0; i < columnB.keyValues.size(); i++) {
            columnMapB.put(columnB.keyValues.get(i), columnB.columnValues[i]);
        }

        // loop over column B creating to new vectorsindex for primary key in column B
        DoubleList joinValuesA = new DoubleArrayList();
        DoubleList joinValuesB = new DoubleArrayList();
        for (int i = 0; i < columnA.keyValues.size(); i++) {
            String keyA = columnA.keyValues.get(i);
            double valueA = columnA.columnValues[i];
            List<Double> rowsB = columnMapB.get(keyA);
            if (rowsB != null && !rowsB.isEmpty()) {
                for (Double valueB : rowsB) {
                    joinValuesA.add(valueA);
                    joinValuesB.add(valueB);
                }
            }
        }

        if (joinValuesA.isEmpty()) {
            return Double.NaN;
        }

        return PearsonCorrelation.coefficient(
                joinValuesA.toDoubleArray(),
                joinValuesB.toDoubleArray()
        );
    }

}
