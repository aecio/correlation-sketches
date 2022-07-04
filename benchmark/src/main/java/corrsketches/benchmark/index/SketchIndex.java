package corrsketches.benchmark.index;

import corrsketches.CorrelationSketch;
import corrsketches.CorrelationSketch.ImmutableCorrelationSketch;
import corrsketches.SketchType;
import corrsketches.benchmark.ColumnPair;
import corrsketches.benchmark.IndexCorrelationBenchmark.SortBy;
import corrsketches.benchmark.index.Hit.RerankStrategy;
import corrsketches.correlation.PearsonCorrelation;
import corrsketches.kmv.ValueHash;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeSet;
import org.apache.lucene.document.*;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.BooleanQuery.Builder;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;

public class SketchIndex extends AbstractLuceneIndex {

  protected static final String HASHES_FIELD_NAME = "h";
  protected static final String VALUES_FIELD_NAME = "v";
  protected static final String ID_FIELD_NAME = "i";

  protected final CorrelationSketch.Builder builder;
  protected final RerankStrategy reranker;
  protected final boolean sort;

  public SketchIndex() throws IOException {
    this(SketchType.KMV, 256);
  }

  public SketchIndex(SketchType sketchType, double threshold) throws IOException {
    this(null, sketchType, threshold);
  }

  public SketchIndex(String indexPath, SketchType sketchType, double threshold) throws IOException {
    this(
        indexPath,
        CorrelationSketch.builder().sketchType(sketchType, threshold),
        SortBy.CSK,
        false);
  }

  public SketchIndex(
      String indexPath, CorrelationSketch.Builder builder, SortBy sortBy, boolean readonly)
      throws IOException {
    super(indexPath, readonly);
    this.builder = builder;
    if (sortBy != SortBy.KEY) {
      this.sort = true;
      this.reranker = sortBy.reranker;
    } else {
      this.sort = false;
      this.reranker = null;
    }
  }

  public void index(String id, ColumnPair columnPair) throws IOException {

    final ImmutableCorrelationSketch sketch =
        builder.build(columnPair.keyValues, columnPair.columnValues).toImmutable();

    final int[] keys = sketch.getKeys();
    final double[] values = sketch.getValues();

    Document doc = new Document();
    doc.add(new StringField(ID_FIELD_NAME, id, Field.Store.YES));

    // store and index sketch data in the document
    indexAndStoreIntArray(doc, HASHES_FIELD_NAME, keys);
    storeDoubleArray(doc, VALUES_FIELD_NAME, values);

    writer.updateDocument(new Term(ID_FIELD_NAME, id), doc);
    // refresh();
  }

  public List<Hit> search(ColumnPair columnPair, int k) throws IOException {

    CorrelationSketch querySketch = builder.build(columnPair.keyValues, columnPair.columnValues);
    querySketch.setCardinality(columnPair.keyValues.size());

    Builder bq = new BooleanQuery.Builder();
    TreeSet<ValueHash> kMinValues = querySketch.getKMinValues();
    for (ValueHash vh : kMinValues) {
      final Term term = new Term(HASHES_FIELD_NAME, intToBytesRef(vh.keyHash));
      bq.add(new TermQuery(term), Occur.SHOULD);
    }

    return executeQuery(k, querySketch.toImmutable(), bq.build());
  }

  protected List<Hit> executeQuery(int k, ImmutableCorrelationSketch cs, Query query)
      throws IOException {
    IndexSearcher searcher = searcherManager.acquire();
    try {
      TopDocs hits = searcher.search(query, k);
      List<Hit> results = new ArrayList<>();
      for (int i = 0; i < hits.scoreDocs.length; i++) {
        final ScoreDoc scoreDoc = hits.scoreDocs[i];
        final Hit hit = createSearchHit(cs, searcher, scoreDoc, this.sort);
        results.add(hit);
      }
      if (this.sort) {
        this.reranker.sort(results);
      }
      return results;
    } finally {
      searcherManager.release(searcher);
    }
  }

  protected Hit createSearchHit(
      ImmutableCorrelationSketch query,
      IndexSearcher searcher,
      ScoreDoc scoreDoc,
      boolean loadSketch)
      throws IOException {
    Document doc = searcher.doc(scoreDoc.doc);
    // retrieve id from index fields
    String id = doc.getValues(ID_FIELD_NAME)[0];
    // read sketch from index fields
    ImmutableCorrelationSketch sketch = loadSketch ? readSketchFromIndex(doc) : null;
    return new Hit(id, query, sketch, scoreDoc.score, scoreDoc.doc, this);
  }

  protected ImmutableCorrelationSketch readSketchFromIndex(Document doc) {
    // re-construct sketch data structures from bytes read from the doc
    int[] hashes = readIntArrayField(doc, HASHES_FIELD_NAME);
    double[] values = readDoubleArrayField(doc, VALUES_FIELD_NAME);
    return new ImmutableCorrelationSketch(hashes, values, PearsonCorrelation::estimate);
  }

  ImmutableCorrelationSketch loadSketch(int docId) throws IOException {
    IndexSearcher searcher = searcherManager.acquire();
    try {
      Document doc = searcher.doc(docId);
      return readSketchFromIndex(doc);
    } finally {
      searcherManager.release(searcher);
    }
  }
}
