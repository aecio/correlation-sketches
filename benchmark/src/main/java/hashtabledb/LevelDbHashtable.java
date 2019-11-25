package hashtabledb;

import static org.iq80.leveldb.impl.Iq80DBFactory.factory;

import com.google.common.base.Preconditions;
import com.google.common.io.BaseEncoding;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.DBException;
import org.iq80.leveldb.Options;

class LevelDbHashtable implements HashtableBackend, Closeable {

  protected Options options;
  protected DB db;

  public LevelDbHashtable(String path) {
    File file = new File(path);
    if (!file.exists()) {
      file.mkdirs();
    }
    this.options = new Options();
    this.options.createIfMissing(true);
    try {
      this.db = factory.open(file, options);
    } catch (IOException e) {
      String message =
          String.format(
              "Failed to open/create RocksDB database at %s. Error message: %s",
              path, e.getMessage());
      throw new RuntimeException(message, e);
    }
  }

  public void putBytes(byte[] keyBytes, byte[] valueBytes) {
    try {
      db.put(keyBytes, valueBytes);
    } catch (DBException e) {
      String hexKey = BaseEncoding.base16().encode(keyBytes);
      throw new RuntimeException("Failed to write key to database: " + hexKey);
    }
  }

  public byte[] getBytes(byte[] keyBytes) {
    Preconditions.checkNotNull(this.db, "Make sure the database is open.");
    byte[] valueBytes;
    try {
      valueBytes = db.get(keyBytes);
    } catch (DBException e) {
      String hexKey = BaseEncoding.base16().encode(keyBytes);
      throw new RuntimeException("Failed to get value from database for key: " + hexKey, e);
    }
    return valueBytes;
  }

  @Override
  public synchronized void close() {
    if (db != null) {
      try {
        db.close();
        db = null;
        options = null;
      } catch (IOException e) {
        throw new RuntimeException("Failed to close database.");
      }
    }
  }

  @Override
  public CloseableIterator<KV<byte[], byte[]>> createIterator() {
    throw new UnsupportedOperationException("Feature not implemented yet");
  }
}
