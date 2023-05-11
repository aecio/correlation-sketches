package corrsketches.benchmark.datasource;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.Sets;
import corrsketches.benchmark.ColumnPair;
import corrsketches.benchmark.CreateColumnStore;
import corrsketches.benchmark.CreateColumnStore.ColumnStoreMetadata;
import corrsketches.benchmark.pairwise.ColumnCombination;
import corrsketches.benchmark.pairwise.TablePair;
import corrsketches.benchmark.utils.ReservoirSampler;
import edu.nyu.engineering.vida.kvdb4j.api.StringObjectKVDB;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

public class DBSource {

  ColumnStoreMetadata storeMetadata;
  StringObjectKVDB<ColumnPair> columnStore;
  Cache<String, ColumnPair> cache = CacheBuilder.newBuilder().softValues().build();

  public DBSource(String inputPath) throws IOException {
    final boolean readonly = true;
    storeMetadata = CreateColumnStore.readMetadata(inputPath);
    columnStore = CreateColumnStore.KVColumnStore.create(inputPath, storeMetadata.dbType, readonly);
    System.out.println(
        "> Found  "
            + storeMetadata.columnSets.size()
            + " column pair sets in DB stored at "
            + inputPath);
  }

  public List<DBColumnCombination> createColumnCombinations(
      Boolean intraDatasetCombinations, int maxColumnsSamples) {

    List<DBColumnCombination> result = new ArrayList<>();

    if (intraDatasetCombinations) {
      for (Set<String> c : storeMetadata.columnSets) {
        Set<Set<String>> intraCombinations = Sets.combinations(c, 2);
        for (Set<String> columnPair : intraCombinations) {
          result.add(createColumnCombination(columnPair));
        }
      }
    } else {
      // If there are more columns than maxColumnsSamples, create a sample of size maxColumnsSamples
      ReservoirSampler<String> sampler = new ReservoirSampler<>(maxColumnsSamples);
      for (Set<String> c : storeMetadata.columnSets) {
        for (String s : c) {
          sampler.sample(s);
        }
      }
      Set<String> columnsSet = new HashSet<>(sampler.getSamples());
      Set<Set<String>> interCombinations = Sets.combinations(columnsSet, 2);
      for (Set<String> columnPair : interCombinations) {
        result.add(createColumnCombination(columnPair));
      }
    }

    return result;
  }

  private DBColumnCombination createColumnCombination(Set<String> columnPair) {
    Iterator<String> it = columnPair.iterator();
    String x = it.next();
    String y = it.next();
    return new DBColumnCombination(x, y);
  }

  private ColumnPair getColumnPair(
      Cache<String, ColumnPair> cache, StringObjectKVDB<ColumnPair> db, String key) {
    ColumnPair cp = cache.getIfPresent(key);
    if (cp == null) {
      cp = db.get(key);
      cache.put(key, cp);
    }
    return cp;
  }

  public void close() {
    columnStore.close();
  }

  public class DBColumnCombination implements ColumnCombination {

    private final String xid;
    private final String yid;

    public DBColumnCombination(String xid, String yid) {
      this.xid = xid;
      this.yid = yid;
    }

    @Override
    public TablePair getTablePair() {
      ColumnPair x = getColumnPair(cache, columnStore, xid);
      ColumnPair y = getColumnPair(cache, columnStore, yid);
      return new TablePair(x, y);
    }
  }
}
