package hashtabledb;

import java.io.Closeable;
import java.nio.charset.StandardCharsets;

public abstract class AbstractDbHashtable implements Closeable {

  protected final HashtableBackend db;

  public AbstractDbHashtable(DBType backend, String path) {
    this(backend, path, false);
  }

  public AbstractDbHashtable(DBType backend, String path, boolean readonly) {
    if (backend == DBType.ROCKSDB) {
      this.db = new RocksDbHashtable(path, readonly);
    } else {
      this.db = new LevelDbHashtable(path);
    }
  }

  protected void putBytes(byte[] keyBytes, byte[] valueBytes) {
    db.putBytes(keyBytes, valueBytes);
  }

  protected byte[] getBytes(byte[] keyBytes) {
    return db.getBytes(keyBytes);
  }

  @Override
  public synchronized void close() {
    db.close();
  }

  /*
   * Converts an int to a byte array using big-endian order.
   */
  static byte[] intToBytes(int value) {
    return new byte[] {
      (byte) (value >> 24), (byte) (value >> 16), (byte) (value >> 8), (byte) (value)
    };
  }

  /*
   * Converts a byte array to an int using big-endian order.
   */
  static int bytesToInt(byte[] bytes) {
    return (bytes[0]) << 24 | (bytes[1] & 0xFF) << 16 | (bytes[2] & 0xFF) << 8 | (bytes[3] & 0xFF);
  }

  static byte[] stringToBytes(String value) {
    return value.getBytes(StandardCharsets.UTF_8);
  }

  static String bytesToString(byte[] bytes) {
    return new String(bytes, StandardCharsets.UTF_8);
  }

  protected CloseableIterator<KV<byte[], byte[]>> createIterator() {
    return db.createIterator();
  }
}
