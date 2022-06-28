package corrsketches.benchmark;

import static org.assertj.core.api.Assertions.assertThat;

import corrsketches.ColumnType;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;

public class TablesTest {

  @Test
  public void shouldFindCSVFiles() throws IOException {
    String directory = TablesTest.class.getResource("TablesTest/csv-files/").getPath();

    final List<String> allCSVs = Tables.findAllTables(directory);
    assertThat(allCSVs).isNotEmpty();

    List<String> filenames =
        allCSVs.stream()
            .map(s -> Paths.get(s).getFileName().toString())
            .collect(Collectors.toList());

    assertThat(filenames).contains("test1.csv");
    assertThat(filenames).contains("test-column-types.csv");
  }

  @Test
  public void shouldReadCSVFileColumnPairsOfNumericalTypes() {
    String csvFile =
        TablesTest.class.getResource("TablesTest/csv-files/test-column-types.csv").getPath();
    final Iterator<ColumnPair> it = Tables.readColumnPairs(csvFile, 0, ColumnType.NUMERICAL);
    assertThat(it.hasNext()).isTrue();

    ColumnPair cp = it.next();
    assertThat(cp.keyName).isEqualTo("char");
    assertThat(cp.columnName).isEqualTo("int");

    cp = it.next();
    assertThat(cp.keyName).isEqualTo("char");
    assertThat(cp.columnName).isEqualTo("double");

    cp = it.next();
    assertThat(cp.keyName).isEqualTo("str");
    assertThat(cp.columnName).isEqualTo("int");

    cp = it.next();
    assertThat(cp.keyName).isEqualTo("str");
    assertThat(cp.columnName).isEqualTo("double");
  }

  @Test
  public void shouldReadCSVFileColumnPairsOfCategoricalTypes() {
    String csvFile =
        TablesTest.class.getResource("TablesTest/csv-files/test-column-types.csv").getPath();
    final Iterator<ColumnPair> it = Tables.readColumnPairs(csvFile, 0, ColumnType.CATEGORICAL);
    assertThat(it.hasNext()).isTrue();

    ColumnPair cp = it.next();
    assertThat(cp.keyName).isEqualTo("char");
    assertThat(cp.columnName).isEqualTo("char");

    cp = it.next();
    assertThat(cp.keyName).isEqualTo("char");
    assertThat(cp.columnName).isEqualTo("str");

    cp = it.next();
    assertThat(cp.keyName).isEqualTo("str");
    assertThat(cp.columnName).isEqualTo("char");

    cp = it.next();
    assertThat(cp.keyName).isEqualTo("str");
    assertThat(cp.columnName).isEqualTo("str");
  }

  @Test
  public void shouldReadCSVFileColumnPairs() {
    String csvFile = TablesTest.class.getResource("TablesTest/csv-files/test1.csv").getPath();
    final Iterator<ColumnPair> it = Tables.readColumnPairs(csvFile, 0, ColumnType.NUMERICAL);
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
    final Iterator<ColumnPair> it = Tables.readColumnPairs(csvFile, 0, ColumnType.NUMERICAL);
    assertThat(it.hasNext()).isTrue();

    ColumnPair cp = it.next();
    assertThat(cp.keyName).isEqualTo("key");
    assertThat(cp.columnName).isEqualTo("value");
  }
}
