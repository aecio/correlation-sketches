package corrsketches.benchmark;

import static org.assertj.core.api.Assertions.assertThat;

import java.net.URISyntaxException;
import java.nio.file.Paths;
import java.util.Iterator;
import java.util.List;
import org.junit.jupiter.api.Test;

public class TablesTest {

  @Test
  public void shouldFindCSVFiles() throws Exception {
    String directory = resolvePath("TablesTest/csv-files/");
    final List<String> allCSVs = Tables.findAllTables(directory);

    assertThat(allCSVs).singleElement().isNotNull();
    assertThat(allCSVs.get(0)).endsWith("test1.csv");
  }

  @Test
  public void shouldReadCSVFileColumnPairs() throws Exception {
    String csvFile = resolvePath("TablesTest/csv-files/test1.csv");
    final Iterator<ColumnPair> it = Tables.readColumnPairs(csvFile, 0);
    assertThat(it.hasNext()).isTrue();

    ColumnPair cp = it.next();
    assertThat(cp.keyName).isEqualTo("key");
    assertThat(cp.columnName).isEqualTo("value");
  }

  @Test
  public void shouldFindParquetFiles() throws Exception {
    String directory = resolvePath("TablesTest/parquet-files/");
    final List<String> allCSVs = Tables.findAllTables(directory);
    assertThat(allCSVs).singleElement().isNotNull();
    assertThat(allCSVs.get(0)).endsWith("test1.parquet");
  }

  @Test
  public void shouldReadParquetFileColumnPairs() throws Exception {
    String csvFile = resolvePath("TablesTest/parquet-files/test1.parquet");
    final Iterator<ColumnPair> it = Tables.readColumnPairs(csvFile, 0);
    assertThat(it.hasNext()).isTrue();

    ColumnPair cp = it.next();
    assertThat(cp.keyName).isEqualTo("key");
    assertThat(cp.columnName).isEqualTo("value");
  }

  /** Resolves paths in OS-independent way. */
  private static String resolvePath(String path) throws URISyntaxException {
    return Paths.get(TablesTest.class.getResource(path).toURI()).toString();
  }
}
