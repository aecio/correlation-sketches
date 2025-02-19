package corrsketches.benchmark;

import static org.assertj.core.api.Assertions.assertThat;

import corrsketches.ColumnType;
import java.net.URISyntaxException;
import java.nio.file.Paths;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;

public class TablesTest {

  @Test
  public void shouldFindCSVFiles() throws Exception {
    String directory = resolvePath("TablesTest/csv-files/");
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
  public void shouldReadCSVFileColumnPairsOfNumericalTypes() throws Exception {
    String csvFile = resolvePath("TablesTest/csv-files/test-column-types.csv");
    final Iterator<ColumnPair> it =
        Tables.readColumnPairs(csvFile, 0, Set.of(ColumnType.NUMERICAL));
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
  public void shouldReadCSVFileColumnPairsOfCategoricalTypes() throws Exception {
    String csvFile = resolvePath("TablesTest/csv-files/test-column-types.csv");
    final Iterator<ColumnPair> it =
        Tables.readColumnPairs(csvFile, 0, Set.of(ColumnType.CATEGORICAL));
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
  public void shouldReadCSVFileColumnPairsOfMixedTypes() throws Exception {
    String csvFile = resolvePath("TablesTest/csv-files/test-column-types.csv");
    final Iterator<ColumnPair> it =
        Tables.readColumnPairs(csvFile, 0, Set.of(ColumnType.CATEGORICAL, ColumnType.NUMERICAL));
    assertThat(it.hasNext()).isTrue();

    ColumnPair cp = it.next();
    assertThat(cp.keyName).isEqualTo("char");
    assertThat(cp.columnName).isEqualTo("char");

    cp = it.next();
    assertThat(cp.keyName).isEqualTo("char");
    assertThat(cp.columnName).isEqualTo("str");

    cp = it.next();
    assertThat(cp.keyName).isEqualTo("char");
    assertThat(cp.columnName).isEqualTo("int");

    cp = it.next();
    assertThat(cp.keyName).isEqualTo("char");
    assertThat(cp.columnName).isEqualTo("double");

    cp = it.next();
    assertThat(cp.keyName).isEqualTo("str");
    assertThat(cp.columnName).isEqualTo("char");

    cp = it.next();
    assertThat(cp.keyName).isEqualTo("str");
    assertThat(cp.columnName).isEqualTo("str");

    cp = it.next();
    assertThat(cp.keyName).isEqualTo("str");
    assertThat(cp.columnName).isEqualTo("int");

    cp = it.next();
    assertThat(cp.keyName).isEqualTo("str");
    assertThat(cp.columnName).isEqualTo("double");
  }

  @Test
  public void shouldReadCSVFileColumnPairs() throws Exception {
    String csvFile = resolvePath("TablesTest/csv-files/test1.csv");
    final Iterator<ColumnPair> it =
        Tables.readColumnPairs(csvFile, 0, Set.of(ColumnType.NUMERICAL));
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
    final Iterator<ColumnPair> it =
        Tables.readColumnPairs(csvFile, 0, Set.of(ColumnType.NUMERICAL));

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
