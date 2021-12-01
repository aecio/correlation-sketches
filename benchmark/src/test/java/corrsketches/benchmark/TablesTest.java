package corrsketches.benchmark;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import org.junit.jupiter.api.Test;

public class TablesTest {

  @Test
  public void shouldFindCSVFiles() throws IOException {
    String directory = TablesTest.class.getResource("TablesTest/csv-files/").getPath();
    final List<String> allCSVs = Tables.findAllTables(directory);

    assertThat(allCSVs).singleElement().isNotNull();
    assertThat(allCSVs.get(0)).endsWith("test1.csv");
  }

  @Test
  public void shouldReadCSVFileColumnPairs() {
    String csvFile = TablesTest.class.getResource("TablesTest/csv-files/test1.csv").getPath();
    final Iterator<ColumnPair> it = Tables.readColumnPairs(csvFile, 0);
    assertThat(it.hasNext()).isTrue();

    ColumnPair cp = it.next();
    assertThat(cp.keyName).isEqualTo("key");
    assertThat(cp.columnName).isEqualTo("value");
  }

  @Test
  public void shouldFindParquetFiles() throws IOException {
    String directory = TablesTest.class.getResource("TablesTest/parquet-files/").getPath();
    final List<String> allCSVs = Tables.findAllTables(directory);
    assertThat(allCSVs).singleElement().isNotNull();
    assertThat(allCSVs.get(0)).endsWith("test1.parquet");
  }

  @Test
  public void shouldReadParquetFileColumnPairs() {
    String csvFile =
        TablesTest.class.getResource("TablesTest/parquet-files/test1.parquet").getPath();
    final Iterator<ColumnPair> it = Tables.readColumnPairs(csvFile, 0);
    assertThat(it.hasNext()).isTrue();

    ColumnPair cp = it.next();
    assertThat(cp.keyName).isEqualTo("key");
    assertThat(cp.columnName).isEqualTo("value");
  }
}
