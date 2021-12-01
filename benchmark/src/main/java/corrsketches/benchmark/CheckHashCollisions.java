package corrsketches.benchmark;

import corrsketches.benchmark.utils.CliTool;
import corrsketches.util.Hashes;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap.Entry;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import java.io.Serializable;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(name = CheckHashCollisions.JOB_NAME, description = "")
public class CheckHashCollisions extends CliTool implements Serializable {

  public static final String JOB_NAME = "CheckHashCollisions";

  @Option(names = "--input-path", required = true, description = "Folder containing CSV files")
  private String inputPath;

  public static void main(String[] args) {
    CliTool.run(args, new CheckHashCollisions());
  }

  @Override
  public void execute() throws Exception {
    List<String> allCSVs = Tables.findAllTables(inputPath);
    System.out.println("> Found  " + allCSVs.size() + " CSV files at " + inputPath);
    Int2ObjectOpenHashMap<TreeSet<String>> allKeys = new Int2ObjectOpenHashMap<>();
    for (String csv : allCSVs) {
      List<Set<String>> keyColumns = Tables.readAllKeyColumns(csv);
      for (Set<String> column : keyColumns) {
        for (String key : column) {
          int h = Hashes.murmur3_32(key);
          TreeSet<String> mappedKeys = allKeys.get(h);
          if (mappedKeys == null) {
            mappedKeys = new TreeSet<>();
            allKeys.put(h, mappedKeys);
          }
          mappedKeys.add(key);
        }
      }
    }
    int totalKeys = 0;
    int totalCollisions = 0;
    int totalHashes = 0;
    for (Entry<TreeSet<String>> hk : allKeys.int2ObjectEntrySet()) {
      int numberOfkeys = hk.getValue().size();
      int hash = hk.getIntKey();
      int collisions = numberOfkeys - 1;
      if (collisions > 0) {
        System.out.printf(
            "hash=%d #-collisions=%d  keys=%s\n", hash, collisions, hk.getValue().toString());
      }
      totalCollisions += collisions;
      totalKeys += numberOfkeys;
      totalHashes++;
    }
    double percent = 100 * totalCollisions / (double) totalKeys;
    System.out.printf(
        "\ntotal-hashes: %d  total-collisions: %d  percent: %.3f%%\n",
        totalHashes, totalCollisions, percent);
  }
}
