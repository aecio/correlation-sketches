package corrsketches.benchmark;

import corrsketches.aggregations.AggregateFunction;
import org.apache.commons.text.StringEscapeUtils;

public class PerfResult implements Cloneable {

  public String parameters;
  public String columnId;
  // Full data statistics
  public ComputingTime time;
  public int cardx_actual;
  public int cardy_actual;
  public int interxy_actual;
  // Sketch statistics
  public long build_x_time;
  public long build_y_time;
  public long build_time;
  public long sketch_join_time;

  public int sketch_join_size;
  public long rp_time;
  public long rqn_time;
  public long rs_time;
  public long rrin_time;
  public long rpm1_time;
  public long rpm1s_time;
  public AggregateFunction aggregate;

  public static String csvHeader() {
    return ""
        // full correlation times
        + "join_time,"
        + "spearmans_time,"
        + "pearsons_time,"
        + "rin_time,"
        + "qn_time,"
        // cardinalities
        + "cardx_actual,"
        + "cardy_actual,"
        + "interxy_actual,"
        // sketch times
        + "build_x_time,"
        + "build_y_time,"
        + "build_time,"
        + "sketch_join_time,"
        // sketch join size
        + "sketch_join_size,"
        // sketch correlation times
        + "rp_time,"
        + "rqn_time,"
        + "rs_time,"
        + "rrin_time,"
        + "rpm1_time,"
        + "rpm1s_time,"
        // others
        + "parameters,"
        + "aggregate,"
        + "column";
  }

  public String csvLine() {
    return String.format(
        ""
            + "%d,%d,%d,%d,%d," // full correlations
            + "%d,%d,%d," // cardinalities
            + "%d,%d,%d,%d," // sketch times
            + "%d," // sketch join size
            + "%d,%d,%d,%d,%d,%d," // sketch correlation times
            + "%s,%s,%s",
        // full correlation times
        time.join,
        time.spearmans,
        time.pearsons,
        time.rin,
        time.qn,
        // cardinalities
        cardx_actual,
        cardy_actual,
        interxy_actual,
        // sketch times
        build_x_time,
        build_y_time,
        build_time,
        sketch_join_time,
        // sketch join size
        sketch_join_size,
        // sketch correlation times
        rp_time,
        rqn_time,
        rs_time,
        rrin_time,
        rpm1_time,
        rpm1s_time,
        // others
        StringEscapeUtils.escapeCsv(parameters),
        aggregate.toString(),
        StringEscapeUtils.escapeCsv(columnId));
  }

  public PerfResult clone() {
    try {
      return (PerfResult) super.clone();
    } catch (CloneNotSupportedException e) {
      throw new IllegalStateException(
          this.getClass() + " must implement the Cloneable interface.", e);
    }
  }

  static class ComputingTime {
    public long join = -1;
    public long spearmans = -1;
    public long pearsons = -1;
    public long rin = -1;
    public long qn = -1;
  }
}
