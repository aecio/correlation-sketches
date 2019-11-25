package hashtabledb;

import java.io.Closeable;
import java.io.UnsupportedEncodingException;

public abstract class AbstractDbHashtable implements Closeable {

  protected HashtableBackend db;

  public AbstractDbHashtable(DBType backend, String path) {
    if (backend == DBType.ROCKSDB) {
      this.db = new RocksDbHashtable(path);
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
    try {
      return value.getBytes("UTF-8");
    } catch (UnsupportedEncodingException e) {
      throw new IllegalStateException("UTF-8 encoding not supported", e);
    }
  }

  static String bytesToString(byte[] bytes) {
    try {
      return new String(bytes, "UTF-8");
    } catch (UnsupportedEncodingException e) {
      throw new IllegalStateException("UTF-8 encoding not supported", e);
    }
  }

  protected CloseableIterator<KV<byte[],byte[]>> createIterator() {
    return db.createIterator();
  }
}
