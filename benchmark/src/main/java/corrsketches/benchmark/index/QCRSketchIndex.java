package corrsketches.benchmark.index;

import corrsketches.CorrelationSketch;
import corrsketches.CorrelationSketch.ImmutableCorrelationSketch;
import corrsketches.SketchType;
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

  public QCRSketchIndex(String indexPath, SketchType sketchType, double threshold)
      throws IOException {
    super(indexPath, sketchType, threshold);
  }

  public QCRSketchIndex() {
    super();
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
    writer.flush();
    searcherManager.maybeRefresh();
  }

  private static int[] computeCorrelationIndexKeys(int[] keys, double[] values) {
    double meanx = Stats.mean(values);
    double stdx = Stats.std(values);
    int[] indexKeys = new int[keys.length];

    int[] signs = new int[keys.length];
    for (int i = 0; i < keys.length; i++) {
      final double q = (values[i] - meanx) / stdx;
      // final double q = (values[i] - meanx);
      if (q > 0.0) {
        signs[i] = 1;
      } else if (q < 0.0) {
        signs[i] = -1;
      } else {
        signs[i] = 0;
      }
    }
    // System.out.println("mean: " + meanx);
    for (int i = 0; i < keys.length; i++) {
      String sign = "=";
      if (signs[i] > 0) {
        sign = "+";
      } else if (signs[i] < 0) {
        sign = "-";
      }
      // System.out.printf("%s ", sign);
      indexKeys[i] = Hashes.murmur3_32(keys[i] + sign);
    }
    // System.out.
    //
    // f("\n");
    // for (int i = 0; i < keys.length; i++) {
    //   System.out.printf("%s ", keys[i]);
    // }
    // System.out.printf("\n");
    // for (int i = 0; i < indexKeys.length; i++) {
    //   System.out.printf("%s ", indexKeys[i]);
    // }
    // System.out.printf("\n");
    return indexKeys;
  }

  public List<Hit> search(ColumnPair columnPair, int k) throws IOException {

    CorrelationSketch query = builder.build(columnPair.keyValues, columnPair.columnValues);
    query.setCardinality(columnPair.keyValues.size());

    IndexSearcher searcher = searcherManager.acquire();
    try {

      final ImmutableCorrelationSketch sketch = query.toImmutable();

      int[] keys = sketch.getKeys();
      // System.out.println("q");
      final double[] values = sketch.getValues();
      int[] posIndexKeys = computeCorrelationIndexKeys(keys, values);

      final double[] flippedValues = new double[values.length];
      for (int i = 0; i < values.length; i++) {
        flippedValues[i] = -1 * values[i];
      }
      int[] negIndexKeys = computeCorrelationIndexKeys(keys, flippedValues);

      Builder bq1 = new BooleanQuery.Builder();
      Builder bq2 = new BooleanQuery.Builder();
      // for (int key : posIndexKeys) {
      //   final Term term = new Term(CORRHASHES_FIELD_NAME, intToBytesRef(key));
      //   bq.add(new TermQuery(term), Occur.SHOULD);
      // }
      for (int i = 0; i < posIndexKeys.length; i++) {
        final int key1 = posIndexKeys[i];
        final Term term1 = new Term(QCR_HASHES_FIELD_NAME, intToBytesRef(key1));
        bq1.add(new TermQuery(term1), Occur.SHOULD);

        final int key2 = negIndexKeys[i];
        final Term term2 = new Term(QCR_HASHES_FIELD_NAME, intToBytesRef(key2));
        bq2.add(new TermQuery(term2), Occur.SHOULD);
      }

      // TreeSet<ValueHash> kMinValues = query.getKMinValues();
      // for (ValueHash vh : kMinValues) {
      //   final Term term = new Term(HASHES_FIELD_NAME, intToBytesRef(vh.keyHash));
      //   bq.add(new TermQuery(term), Occur.SHOULD);
      // }
      // bq.setMinimumNumberShouldMatch(1);
      DisjunctionMaxQuery q = new DisjunctionMaxQuery(Arrays.asList(bq1.build(), bq2.build()), 0f);
      TopDocs hits = searcher.search(q, k);

      List<Hit> results = new ArrayList<>();
      for (int i = 0; i < hits.scoreDocs.length; i++) {
        final ScoreDoc scoreDoc = hits.scoreDocs[i];
        final Hit hit = createSearchHit(sketch, searcher.doc(scoreDoc.doc), scoreDoc.score);
        results.add(hit);
      }

      // results.sort((a, b) -> Double.compare(b.correlationAbsolute(), a.correlationAbsolute()));

      return results;
    } finally {
      searcherManager.release(searcher);
    }
  }
}
