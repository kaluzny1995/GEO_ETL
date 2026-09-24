package sparkmonitor;

import org.apache.spark.scheduler.SparkListener;
import org.apache.spark.scheduler.SparkListenerStageCompleted;
import org.apache.spark.scheduler.StageInfo;
import org.apache.spark.executor.TaskMetrics;

import java.io.FileWriter;
import java.io.PrintWriter;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Spark listener that logs JVM and Spark memory consumption on each stage completion.
 *
 * <p>Output format:
 * {@code 2026-09-24 21:14:37 INFO Stage 0 (count at app.py:10): 4 tasks |
 * JVM Heap: 72 MB / 1024 MB (7.0%) |
 * Spark Exec=0 MB + Storage=0 MB |
 * MemSpilled=0, DiskSpilled=0, GCTime=0ms}
 */
public class StageMemoryListener extends SparkListener {

    private PrintWriter logWriter;
    private final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    public StageMemoryListener(String logPath) {
        try {
            logWriter = new PrintWriter(new FileWriter(logPath, true), true);
        } catch (Exception e) {
            e.printStackTrace();
            logWriter = new PrintWriter(System.out, true);
        }
    }

    @Override
    public void onStageCompleted(SparkListenerStageCompleted stageCompleted) {
        StageInfo si = stageCompleted.stageInfo();
        int stageId = si.stageId();
        String name = si.name();
        int numTasks = si.numTasks();

        long memSpilled = 0;
        long diskSpilled = 0;
        long execRunTime = 0;
        long gcTime = 0;

        TaskMetrics tm = si.taskMetrics();
        if (tm != null) {
            memSpilled = tm.memoryBytesSpilled();
            diskSpilled = tm.diskBytesSpilled();
            execRunTime = tm.executorRunTime();
            gcTime = tm.jvmGCTime();
        }

        Runtime rt = Runtime.getRuntime();
        long jvmUsedMB = (rt.totalMemory() - rt.freeMemory()) / (1024 * 1024);
        long jvmMaxMB = rt.maxMemory() / (1024 * 1024);
        double jvmPct = (jvmMaxMB > 0) ? (100.0 * jvmUsedMB / jvmMaxMB) : 0;

        long sparkExecMB = 0;
        long sparkStorageMB = 0;
        try {
            sparkExecMB = org.apache.spark.SparkEnv.get().memoryManager().executionMemoryUsed() / (1024 * 1024);
            sparkStorageMB = org.apache.spark.SparkEnv.get().memoryManager().storageMemoryUsed() / (1024 * 1024);
        } catch (Exception e) {
            // SparkEnv not initialised yet
        }

        String ts = sdf.format(new Date());
        logWriter.println(String.format(
            "%s INFO Stage %d (%s): %d tasks | JVM Heap: %d MB / %d MB (%.1f%%) | "
            + "Spark Exec=%d MB + Storage=%d MB | "
            + "MemSpilled=%d, DiskSpilled=%d, ExecTime=%dms, GCTime=%dms",
            ts, stageId, name, numTasks,
            jvmUsedMB, jvmMaxMB, jvmPct,
            sparkExecMB, sparkStorageMB,
            memSpilled, diskSpilled, execRunTime, gcTime
        ));
    }
}
