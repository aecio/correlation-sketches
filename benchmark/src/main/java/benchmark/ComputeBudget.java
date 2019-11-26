package benchmark;

import com.github.rvesse.airline.annotations.Command;
import com.github.rvesse.airline.annotations.Option;
import com.github.rvesse.airline.annotations.restrictions.Required;
import hashtabledb.Kryos;
import java.io.Serializable;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import utils.CliTool;

@Command(name = ComputeBudget.JOB_NAME, description = "")
public class ComputeBudget extends CliTool implements Serializable {

  public static final String JOB_NAME = "ComputeBudget";

  public static final Kryos<ColumnPair> KRYO = new Kryos(ColumnPair.class);

  @Required
  @Option(name = "--input-path", description = "Folder containing CSV files")
  private String inputPath;

  public static void main(String[] args) {
    CliTool.run(args, new ComputeBudget());
  }

  @Override
  public void execute() throws Exception {
    List<String> allCSVs = BenchmarkUtils.findAllCSVs(inputPath);
    System.out.println("> Found  " + allCSVs.size() + " CSV files at " + inputPath);

    int[] kValues = new int[] {128, 256, 512, 1024};

    int numberOfKeyColumns = 0;
    int numberOfKeyElements = 0;
    Set<String> uniqueElements = new HashSet<>();
    for (String csv : allCSVs) {
      List<Set<String>> keyColumns = BenchmarkUtils.readAllKeyColumns(csv);
      numberOfKeyColumns += keyColumns.size();
      for (Set<String> column : keyColumns) {
        uniqueElements.addAll(column);
        numberOfKeyElements += column.size();
      }
    }
    int numberOfUniqueElements = uniqueElements.size();

    System.out.println();
    int m = numberOfKeyColumns;
    int N = numberOfKeyElements;
    for (int k : kValues) {
      int b = k * m;
      double tau = b / (double) N;
      double tauUnique = b / (double) numberOfUniqueElements;
      System.out.printf(
          "k=%d budget=%d*%d=%d tau=%.3f tau-unique=%.3f\n", k, k, m, b, tau, tauUnique);
    }
  }
}
