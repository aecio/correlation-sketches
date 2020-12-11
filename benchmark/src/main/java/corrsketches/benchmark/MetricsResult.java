package corrsketches.benchmark;

import corrsketches.aggregations.AggregateFunction;
import corrsketches.benchmark.PerfResult.ComputingTime;
import org.apache.commons.text.StringEscapeUtils;

public class MetricsResult implements Cloneable {

  // kmv estimation statistics
  public double jcx_est;
  public double jcy_est;
  public double jsxy_est;
  public double jcx_actual;
  public double jcy_actual;
  public double jsxy_actual;
  // cardinality statistics
  public double cardx_est;
  public double cardy_est;
  public int cardx_actual;
  public int cardy_actual;
  // set statistics
  public double interxy_est;
  public double unionxy_est;
  public int interxy_actual;
  public int unionxy_actual;
  // Correlation sample size
  public int corr_est_sample_size;
  // Person's correlation
  public double corr_rp_actual;
  public double corr_rp_est;
  public double corr_rp_delta;
  // Pearson's Fisher CI
  //    public double corr_rp_est_pvalue2t;
  //    public ConfidenceInterval corr_rp_est_fisher;
  //    public boolean corr_est_significance;
  // Qn correlation
  public double corr_rqn_actual;
  public double corr_rqn_est;
  public double corr_rqn_delta;
  // Spearman correlation
  public double corr_rs_est;
  public double corr_rs_actual;
  public double corr_rs_delta;
  // RIN correlation
  public double corr_rin_est;
  public double corr_rin_actual;
  public double corr_rin_delta;
  // PM1 bootstrap
  public double corr_pm1_mean;
  public double corr_pm1_mean_delta;
  public double corr_pm1_median;
  public double corr_pm1_median_delta;
  public double corr_pm1_lb;
  public double corr_pm1_ub;
  // Kurtosis
  public double kurtx_g2_actual;
  public double kurty_g2_actual;
  public double kurtx_g2;
  public double kurtx_G2;
  public double kurtx_k5;
  public double kurty_g2;
  public double kurty_G2;
  public double kurty_k5;
  // Variable sample extents
  public double y_min_sample;
  public double y_max_sample;
  public double x_min_sample;
  public double x_max_sample;
  // Variable extents
  public double x_min;
  public double x_max;
  public double y_min;
  public double y_max;
  // Variable sample means and variances
  public double x_sample_mean;
  public double y_sample_mean;
  public double x_sample_var;
  public double y_sample_var;
  // Sum of squares of variables' samples
  public double nu_xy;
  public double nu_x;
  public double nu_y;
  // others
  public String parameters;
  public String columnId;
  public ComputingTime time;
  public AggregateFunction aggregate;

  public static String csvHeader() {
    return String.format(
        ""
            // jaccard
            + "jcx_est,"
            + "jcy_est,"
            + "jcx_actual,"
            + "jcy_actual,"
            + "jsxy_est,"
            + "jsxy_actual,"
            // cardinalities
            + "cardx_est,"
            + "cardx_actual,"
            + "cardy_est,"
            + "cardy_actual,"
            // set statistics
            + "interxy_est,"
            + "interxy_actual,"
            + "unionxy_est,"
            + "unionxy_actual,"
            // Correlation sample size
            + "corr_est_sample_size,"
            // Pearson's correlations
            + "corr_rp_est,"
            + "corr_rp_actual,"
            + "corr_rp_delta,"
            // Pearson's Fisher CI
            //              + "corr_rp_est_pvalue2t,"
            //              + "corr_rp_est_fisher_ub,"
            //              + "corr_rp_est_fisher_lb,"
            // Qn correlations
            + "corr_rqn_est,"
            + "corr_rqn_actual,"
            + "corr_rqn_delta,"
            // Spearman correlations
            + "corr_rs_est,"
            + "corr_rs_actual,"
            + "corr_rs_delta,"
            // RIN correlations
            + "corr_rin_est,"
            + "corr_rin_actual,"
            + "corr_rin_delta,"
            // PM1 bootstrap
            + "corr_pm1_mean,"
            + "corr_pm1_mean_delta,"
            + "corr_pm1_median,"
            + "corr_pm1_median_delta,"
            + "corr_pm1_lb,"
            + "corr_pm1_ub,"
            // Kurtosis
            + "kurtx_g2_actual,"
            + "kurtx_g2,"
            + "kurtx_G2,"
            + "kurtx_k5,"
            + "kurty_g2_actual,"
            + "kurty_g2,"
            + "kurty_G2,"
            + "kurty_k5,"
            // Variable sample extents
            + "y_min_sample,"
            + "y_max_sample,"
            + "x_min_sample,"
            + "x_max_sample,"
            // Variable extents
            + "x_min,"
            + "x_max,"
            + "y_min,"
            + "y_max,"
            // Variable sample means and variances
            + "x_sample_mean,"
            + "y_sample_mean,"
            + "x_sample_var,"
            + "y_sample_var,"
            // Sum of squares of variables' samples
            + "nu_xy,"
            + "nu_x,"
            + "nu_y,"
            // others
            + "parameters,"
            + "aggregate,"
            + "column");
  }

  public String csvLine() {
    return String.format(
        ""
            + "%.3f,%.3f,%.3f,%.3f,%.3f,%.3f," // Jaccard
            + "%.2f,%d,%.2f,%d," // cardinalities
            + "%.2f,%d,%.2f,%d," // set statistics
            + "%d," // sample size
            + "%.3f,%.3f,%.3f," // Pearson's
            //              + "%.3f,%.3f,%.3f," // Pearson's Fisher CI
            + "%.3f,%.3f,%.3f," // Qn
            + "%.3f,%.3f,%.3f," // Spearman's
            + "%.3f,%.3f,%.3f," // RIN
            + "%.3f,%.3f,%.3f,%.3f,%.3f,%.3f," // PM1
            + "%.3f,%.3f,%.3f,%.3f,%.3f,%.3f,%.3f,%.3f," // Kurtosis
            + "%f,%f,%f,%f," // Variable sample extents
            + "%f,%f,%f,%f," // Variable extents
            + "%f,%f,%f,%f," // Variable sample means and variances
            + "%f,%f,%f," // Sum of squares of variables' samples
            + "%s,%s,%s",
        // jaccard
        jcx_est,
        jcy_est,
        jcx_actual,
        jcy_actual,
        jsxy_est,
        jsxy_actual,
        // cardinalities
        cardx_est,
        cardx_actual,
        cardy_est,
        cardy_actual,
        // set statistics
        interxy_est,
        interxy_actual,
        unionxy_est,
        unionxy_actual,
        // sample
        corr_est_sample_size,
        // Pearson's correlations
        corr_rp_est,
        corr_rp_actual,
        corr_rp_delta,
        // Pearson's Fisher CI
        //          corr_rp_est_pvalue2t,
        //          corr_rp_est_fisher.lowerBound,
        //          corr_rp_est_fisher.upperBound,
        // Qn correlations
        corr_rqn_est,
        corr_rqn_actual,
        corr_rqn_delta,
        // Spearman's correlations
        corr_rs_est,
        corr_rs_actual,
        corr_rs_delta,
        // RIN correlations
        corr_rin_est,
        corr_rin_actual,
        corr_rin_delta,
        // PM1 bootstrap
        corr_pm1_mean,
        corr_pm1_mean_delta,
        corr_pm1_median,
        corr_pm1_median_delta,
        corr_pm1_lb,
        corr_pm1_ub,
        // Kurtosis
        kurtx_g2_actual,
        kurtx_g2,
        kurtx_G2,
        kurtx_k5,
        kurty_g2_actual,
        kurty_g2,
        kurty_G2,
        kurty_k5,
        // Variable sample extents
        y_min_sample,
        y_max_sample,
        x_min_sample,
        x_max_sample,
        // Variable extents
        x_min,
        x_max,
        y_min,
        y_max,
        // Variable sample means and variances
        x_sample_mean,
        y_sample_mean,
        x_sample_var,
        y_sample_var,
        // Sum of squares of variables' samples
        nu_xy,
        nu_x,
        nu_y,
        // others
        StringEscapeUtils.escapeCsv(parameters),
        aggregate.toString(),
        StringEscapeUtils.escapeCsv(columnId));
  }

  public MetricsResult clone() {
    try {
      return (MetricsResult) super.clone();
    } catch (CloneNotSupportedException e) {
      throw new IllegalStateException(
          this.getClass() + " must implement the Cloneable interface.", e);
    }
  }
}
