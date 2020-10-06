package hashtabledb;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import java.io.ByteArrayOutputStream;

/**
 * A thread-safe wrapper for serialization using hashtabledb.Kryos.
 *
 * @author aeciosantos
 * @param <T> The type of the object which will be serialized/unserialized.
 */
public class Kryos<T> {

  private static final ThreadLocal<Kryo> KRYOS =
      new ThreadLocal<Kryo>() {
        protected Kryo initialValue() {
          return new Kryo();
        }
      };

  private Class<T> contentClass;

  public Kryos(Class<T> contentClass) {
    this.contentClass = contentClass;
  }

  public byte[] serializeObject(T value) {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    Output output = new Output(baos);
    KRYOS.get().writeObject(output, value);
    output.flush();
    return baos.toByteArray();
  }

  public T unserializeObject(byte[] value) {
    if (value == null) return null;
    Input input = new Input(value);
    return KRYOS.get().readObject(input, contentClass);
  }
}
