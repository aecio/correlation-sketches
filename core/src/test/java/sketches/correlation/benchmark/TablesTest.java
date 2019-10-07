package sketches.correlation.benchmark;

import static org.junit.jupiter.api.Assertions.*;

import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.Test;

public class TablesTest {

  @Test
  public void shouldJoinTablesAndComputeCorrelation() {
    List<String> keyA = Arrays.asList(new String[] {"a", "b", "c", "d", "e"});
    double[] valuesA = new double[] {1.0, 2.0, 3.0, 4.0, 5.0};

    List<String> keyB = Arrays.asList(new String[] {"a", "b", "c", "d"});
    double[] valuesB = new double[] {1.0, 2.0, 3.0, 4.0};

    List<String> keyC = Arrays.asList(new String[] {"a", "b", "c", "z", "x"});
    double[] valuesC = new double[] {0., 0., 3.0, 4.0, 5.0};

    List<String> keyD = Arrays.asList(new String[] {"a", "b", "c", "z"});
    double[] valuesD = new double[] {-1., -2., -3.0, 4.0};

    ColumnPair columnA = new ColumnPair("A", "pk_a", keyA, "values_a", valuesA);
    ColumnPair columnB = new ColumnPair("B", "fk_b", keyB, "values_b", valuesB);
    ColumnPair columnC = new ColumnPair("C", "fk_c", keyC, "values_c", valuesC);
    ColumnPair columnD = new ColumnPair("D", "fk_d", keyD, "values_d", valuesD);

    double delta = 0.0001;
    assertEquals(1.000, Tables.computePearsonAfterJoin(columnA, columnB), delta);
    assertEquals(1.000, Tables.computePearsonAfterJoin(columnB, columnA), delta);

    assertEquals(0.866, Tables.computePearsonAfterJoin(columnB, columnC), delta);
    assertEquals(0.866, Tables.computePearsonAfterJoin(columnC, columnB), delta);

    assertEquals(-1.000, Tables.computePearsonAfterJoin(columnB, columnD), delta);
    assertEquals(-1.000, Tables.computePearsonAfterJoin(columnD, columnB), delta);
  }
}
