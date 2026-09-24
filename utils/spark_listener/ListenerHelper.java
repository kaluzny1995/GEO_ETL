package sparkmonitor;

import org.apache.spark.SparkContext;
import org.apache.spark.scheduler.SparkListener;

/**
 * Java bridge that allows Py4J to register a {@link SparkListener} with SparkContext.
 *
 * <p>Py4J cannot walk the class hierarchy from Python to the Scala abstract class
 * {@code SparkListener}, so this helper does the cast on the JVM side.
 */
public class ListenerHelper {

    public static void addListener(SparkContext sc, Object listener) {
        if (listener instanceof SparkListener) {
            sc.addSparkListener((SparkListener) listener);
        } else {
            throw new IllegalArgumentException(
                "Listener must extend SparkListener, got: " + listener.getClass());
        }
    }
}
