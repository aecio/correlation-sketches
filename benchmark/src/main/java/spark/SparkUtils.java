package spark;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.TreeSet;
import org.apache.spark.SparkConf;

public class SparkUtils {

  public static SparkConf createSparkConf(String appName, String sparkConfParam) {
    return createSparkConf(appName, sparkConfParam, new Class[] {});
  }

  public static SparkConf createSparkConf(
      String appName, String sparkConfParam, Class<?>... classes) {
    System.out.println("\nCreating Spark Configuraation:");
    System.out.println(sparkConfParam + "\n");
    SparkConf conf =
        new SparkConf()
            .setAppName(appName)
            .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
            .registerKryoClasses(
                new Class[] {
                  ArrayList.class, HashMap.class, TreeSet.class, HashSet.class, String.class
                });

    if (classes != null && classes.length > 0) {
      conf.registerKryoClasses(classes);
    }

    if (sparkConfParam != null && !sparkConfParam.isEmpty()) {
      System.out.println("----");
      System.out.println("Setting properties:" + sparkConfParam);
      System.out.println("----");
      String[] props = sparkConfParam.split(";", -1);
      System.out.println(Arrays.deepToString(props));
      if (props != null && props.length > 0) {
        for (int i = 0; i < props.length; i++) {
          String[] kv = props[i].split("=", -1);
          if (kv != null && kv.length == 2) {
            conf.set(kv[0], kv[1]);
            System.out.println(" - " + kv[0] + " : " + kv[1]);
          } else {
            throw new IllegalArgumentException(
                "Invalid config key values: " + Arrays.deepToString(kv));
          }
        }
      }
      System.out.println("----");
    }
    return conf;
  }
}
