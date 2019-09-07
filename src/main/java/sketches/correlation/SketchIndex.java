package sketches.correlation;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeSet;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.BooleanQuery.Builder;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.SearcherFactory;
import org.apache.lucene.search.SearcherManager;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.similarities.BooleanSimilarity;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.BytesRef;
import sketches.correlation.KMVCorrelationSketch.ValueHash;

public class SketchIndex {

    public static final String HASHES_FIELD_NAME = "hashes";
    private static final String VALUES_FIELD_NAME = "values";
    public static final String ID_FIELD_NAME = "id";

    private final IndexWriter writer;
    private final SearcherManager searcherManager;

    public SketchIndex(String indexPath) throws IOException {
        this(MMapDirectory.open(Paths.get(indexPath)));
    }

    public SketchIndex() {
        this(new RAMDirectory());
    }

    public SketchIndex(Directory dir) {
        final Analyzer analyzer = new StandardAnalyzer();
        final IndexWriterConfig iwc = new IndexWriterConfig(analyzer);
        iwc.setOpenMode(OpenMode.CREATE_OR_APPEND);

        final BooleanSimilarity similarity = new BooleanSimilarity();
        iwc.setSimilarity(similarity);

        // Optional: for better indexing performance, if you
        // are indexing many documents, increase the RAM
        // buffer.  But if you do this, increase the max heap
        // size to the JVM (eg add -Xmx512m or -Xmx1g):
        //
        // iwc.setRAMBufferSizeMB(256.0);
        try {
            this.writer = new IndexWriter(dir, iwc);
        } catch (IOException e) {
            throw new RuntimeException("Failed to create index writer", e);
        }

        try {
            SearcherFactory searcherFactory = new SearcherFactory() {
                public IndexSearcher newSearcher(IndexReader reader, IndexReader previousReader)
                        throws IOException {
                    IndexSearcher is = new IndexSearcher(reader);
                    is.setSimilarity(similarity);
                    return is;
                }
            };
            this.searcherManager = new SearcherManager(writer, searcherFactory);
        } catch (IOException e) {
            throw new RuntimeException("Failed to create search manager", e);
        }
    }

    public void index(String id, KMVCorrelationSketch sketch) throws IOException {
        Document doc = new Document();

        // Add the path of the file as a field named "path".  Use a
        // field that is indexed (i.e. searchable), but don't tokenize
        // the field into separate words and don't index term frequency
        // or positional information:
        Field idField = new StringField(ID_FIELD_NAME, id, Field.Store.YES);
        doc.add(idField);

        TreeSet<ValueHash> hashes = sketch.getKMinValues();
        for (ValueHash hash : hashes) {
            doc.add(
                    new StringField(
                            HASHES_FIELD_NAME,
                            String.valueOf(hash.hashValue),
                            Store.YES
                    )
            );
        }

        double[] values = hashes.stream().mapToDouble(v -> v.value).toArray();
        byte[] valuesBytes = toByteArray(values);
        doc.add(new StoredField(VALUES_FIELD_NAME, valuesBytes));

//        System.out.println(Arrays.asList(doc.getValues(HASHES_FIELD_NAME)));
//        System.out.println(Arrays.asList(doc.getValues(VALUES_FIELD_NAME)));
        writer.updateDocument(new Term("id", id), doc);
        writer.flush();
        searcherManager.maybeRefresh();
    }

    public List<Hit> search(KMVCorrelationSketch query, int k) throws IOException {
        IndexSearcher searcher = searcherManager.acquire();
        try {
            Builder bq = new BooleanQuery.Builder();
            TreeSet<ValueHash> kMinValues = query.getKMinValues();
            for (ValueHash vh : kMinValues) {
                bq.add(
                        new TermQuery(
                                new Term(HASHES_FIELD_NAME, String.valueOf(vh.hashValue))
                        ),
                        Occur.SHOULD
                );
            }
//            bq.setMinimumNumberShouldMatch(1);

//            System.out.println("Query: " + bq.build());
            TopDocs hits = searcher.search(bq.build(), k);
//            TopDocs hits = searcher.search(new MatchAllDocsQuery(), k);

            List<Hit> results = new ArrayList<>();
//            System.out.println(hits.totalHits);
            for (int i = 0; i < hits.scoreDocs.length; i++) {
//                System.out.println(hits.scoreDocs[i]);

                Document doc = searcher.doc(hits.scoreDocs[i].doc);
                String id = doc.getValues(ID_FIELD_NAME)[0];
                String[] hashes = doc.getValues(HASHES_FIELD_NAME);
                BytesRef[] valuesRef = doc.getBinaryValues(VALUES_FIELD_NAME);
                double[] values = toDoubleArray(valuesRef[0].bytes);
                results.add(new Hit(id, KMVCorrelationSketch.fromStringHashes(hashes, values)));
            }
            return results;
        } finally {
            searcherManager.release(searcher);
        }
    }

    protected static byte[] toByteArray(double[] value) {
        byte[] bytes = new byte[8 * value.length];
        ByteBuffer bb = ByteBuffer.wrap(bytes);
        for (int i = 0; i < value.length; i++) {
            bb.putDouble(value[i]);
        }
        return bytes;
    }

    protected static double[] toDoubleArray(byte[] bytes) {
        int n = bytes.length / 8;
        double[] doubles = new double[n];
        ByteBuffer bb = ByteBuffer.wrap(bytes);
        for (int i = 0; i < n; i++) {
            doubles[i] = bb.getDouble();
        }
        return doubles;
    }

    public static class Hit {

        final public String id;
        final public KMVCorrelationSketch sketch;

        public Hit(String id, KMVCorrelationSketch sketch) {
            this.id = id;
            this.sketch = sketch;
        }
    }

}
