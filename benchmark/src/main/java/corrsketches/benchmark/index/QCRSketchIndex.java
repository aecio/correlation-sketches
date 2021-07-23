package corrsketches.benchmark.index;

import corrsketches.CorrelationSketch;
import corrsketches.CorrelationSketch.ImmutableCorrelationSketch;
import corrsketches.benchmark.ColumnPair;
import corrsketches.statistics.Stats;
import corrsketches.util.Hashes;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.BooleanQuery.Builder;
import org.apache.lucene.search.DisjunctionMaxQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;

public class QCRSketchIndex extends SketchIndex {

  private static final String QCR_HASHES_FIELD_NAME = "c";

  public QCRSketchIndex() throws IOException {
    super();
  }

  public QCRSketchIndex(String indexPath, CorrelationSketch.Builder builder) throws IOException {
    super(indexPath, builder);
  }

  public void index(String id, ColumnPair columnPair) throws IOException {

    CorrelationSketch sketch = super.builder.build(columnPair.keyValues, columnPair.columnValues);

    Document doc = new Document();

    Field idField = new StringField(ID_FIELD_NAME, id, Field.Store.YES);
    doc.add(idField);

    final ImmutableCorrelationSketch immutable = sketch.toImmutable();

    final int[] keys = immutable.getKeys();
    final double[] values = immutable.getValues();

    // System.out.println(id);
    int[] indexKeys = computeCorrelationIndexKeys(keys, values);

    for (int key : indexKeys) {
      doc.add(new StringField(QCR_HASHES_FIELD_NAME, intToBytesRef(key), Field.Store.NO));
    }

    // add keys to document
    for (int key : keys) {
      doc.add(new StringField(HASHES_FIELD_NAME, intToBytesRef(key), Field.Store.YES));
    }

    // add values to documents
    byte[] valuesBytes = toByteArray(values);
    doc.add(new StoredField(VALUES_FIELD_NAME, valuesBytes));

    writer.updateDocument(new Term(ID_FIELD_NAME, id), doc);
    //    refresh();
  }

  private static int[] computeCorrelationIndexKeys(int[] keys, double[] values) {
    double meanx = Stats.mean(values);
    double stdx = Stats.std(values);

    //    System.out.println("mean: " + meanx);

    int[] indexKeys = new int[keys.length];
    for (int i = 0; i < keys.length; i++) {
      final double q = (values[i] - meanx) / stdx;
      int sign = 0;
      if (q > 0.0) {
        sign = 1;
      } else if (q < 0.0) {
        sign = -1;
      }
      indexKeys[i] = Hashes.MURMUR3.newHasher().putInt(keys[i]).putInt(sign).hash().asInt();

      // DEBUG
      //      String signStr = "=";
      //      if (sign > 0) {
      //        signStr = "+";
      //      } else if (sign < 0) {
      //        signStr = "-";
      //      }
      //      System.out.printf("%d\t[%s]\t%s\n", keys[i], signStr, indexKeys[i]);
    }
    //    System.out.printf("\n");
    return indexKeys;
  }

  public List<Hit> search(ColumnPair columnPair, int k) throws IOException {

    CorrelationSketch query = builder.build(columnPair.keyValues, columnPair.columnValues);
    query.setCardinality(columnPair.keyValues.size());

    final ImmutableCorrelationSketch sketch = query.toImmutable();

    // System.out.println("q");
    final int[] keys = sketch.getKeys();
    final double[] values = sketch.getValues();
    int[] posIndexKeys = computeCorrelationIndexKeys(keys, values);

    final double[] flippedValues = new double[values.length];
    for (int i = 0; i < values.length; i++) {
      flippedValues[i] = -1 * values[i];
    }
    int[] negIndexKeys = computeCorrelationIndexKeys(keys, flippedValues);

    Builder bq1 = new BooleanQuery.Builder();
    Builder bq2 = new BooleanQuery.Builder();
    for (int i = 0; i < posIndexKeys.length; i++) {
      final int key1 = posIndexKeys[i];
      final Term term1 = new Term(QCR_HASHES_FIELD_NAME, intToBytesRef(key1));
      bq1.add(new TermQuery(term1), Occur.SHOULD);

      final int key2 = negIndexKeys[i];
      final Term term2 = new Term(QCR_HASHES_FIELD_NAME, intToBytesRef(key2));
      bq2.add(new TermQuery(term2), Occur.SHOULD);
    }

    DisjunctionMaxQuery q = new DisjunctionMaxQuery(Arrays.asList(bq1.build(), bq2.build()), 0f);

    IndexSearcher searcher = searcherManager.acquire();
    try {
      TopDocs hits = searcher.search(q, k);
      List<Hit> results = new ArrayList<>();
      for (int i = 0; i < hits.scoreDocs.length; i++) {
        final ScoreDoc scoreDoc = hits.scoreDocs[i];
        final Hit hit = createSearchHit(sketch, searcher, scoreDoc, false);
        results.add(hit);
      }
      return results;
    } finally {
      searcherManager.release(searcher);
    }
  }
}
