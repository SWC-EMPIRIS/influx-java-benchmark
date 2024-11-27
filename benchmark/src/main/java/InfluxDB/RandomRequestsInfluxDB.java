package InfluxDB;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.lang.management.ManagementFactory;
import com.sun.management.OperatingSystemMXBean;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.time.format.DateTimeFormatter;
import java.time.LocalDateTime;

import com.influxdb.client.InfluxDBClient;
import com.influxdb.client.InfluxDBClientFactory;
import com.influxdb.client.write.Point;
import com.influxdb.query.FluxTable;
import com.influxdb.client.domain.WritePrecision;

public class RandomRequestsInfluxDB implements Runnable {
    private static int numberOfRequests;
    private static String token;
    private static String bucket;
    private static String org;
    private static String host_url;
    private static int timeRangeMinutesStart;
    private static int timeRangeMinutesEnd;
    private static String influx_data_path;
    private static File storage_Folder;

    private boolean isWriteOperation;

    // Unique CSV filenames with timestamp (one for metrics and one for CPU and RAM usage)
    private static final String csvFileNameMetrics = "data.csv"; //"metrics_log_" + DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss").format(LocalDateTime.now()) + "_1.csv";
    private static final String csvFileNamePerformance = "metrics_log_" + DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss").format(LocalDateTime.now()) + "_2.csv";

    public RandomRequestsInfluxDB(boolean isWriteOperation) {
        this.isWriteOperation = isWriteOperation;
    }

    private synchronized void logToCSVMetrics(String operationType, long latency, int dataPoints) {
        try (FileWriter fileWriter = new FileWriter(csvFileNameMetrics, true);
             PrintWriter printWriter = new PrintWriter(fileWriter)) {

            long timestamp = Instant.now().toEpochMilli();
            printWriter.printf("%d,%s,%d,%d%n", timestamp, operationType, latency, dataPoints);

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static synchronized void logToCSVPerformance() {
        try (FileWriter fileWriter = new FileWriter(csvFileNamePerformance, true);
             PrintWriter printWriter = new PrintWriter(fileWriter)) {

            long timestamp = Instant.now().toEpochMilli();
            printWriter.printf("%d,%d,%f,%d,%d%n",timestamp, getFolderSize(storage_Folder) / 1024, getCpuLoad(), getFreeMemory(), getTotalMemory() );

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    //Measures the total size of files in the given directory.
    public static long getFolderSize(File folder) {
        long length = 0;
        File[] files = folder.listFiles();
        if (files != null) {
            for (File file : files) {
                if (file.isFile()) {
                    length += file.length();
                } else {
                    length += getFolderSize(file);
                }
            }
        }
        return length;
    }

    //necessary for CPU and RAM
    static OperatingSystemMXBean osBean = (OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();

    //Measures the current CPU usage as a value between 0.0 and 1.0.
    static double getCpuLoad(){
        return osBean.getProcessCpuLoad();
    }

    static long getFreeMemory(){
        return osBean.getFreeMemorySize() / (1024 * 1024);
    }
    static long getTotalMemory(){
        return osBean.getTotalMemorySize() / (1024 * 1024);
    }

    /**
     * Populates the given bucket with a timestamp and a random integer.
     *
     * @param bucketParameter               Bucket
     * @param orgParameter                  Influx organization
     * @param tokenParameter                Influx token
     * @param numberOfRequestsParameter     Number of requests
     * @param numberOfThreads               Number of threads
     * @param host_URL                      Host URL
     */
    public static void sendRandomRequests(String bucketParameter, String orgParameter, String tokenParameter, int numberOfRequestsParameter, int numberOfThreads, String host_URL, int writeRatio, int readRatio, int timeRangeMinutesStartParam, int timeRangeMinutesEndParam, String influxDataPath) {
        token = tokenParameter;
        bucket = bucketParameter;
        org = orgParameter;
        numberOfRequests = numberOfRequestsParameter;
        host_url = host_URL;
        timeRangeMinutesStart = timeRangeMinutesStartParam;
        timeRangeMinutesEnd = timeRangeMinutesEndParam;
        influx_data_path = influxDataPath;


        File latestDirectory = null;
        File directory = new File(influx_data_path);


        // Searches for the newest folder in the Influx storage directory
        // In the future, possibly query directly with Influx CLI, as bucket name != storage ID
        if (directory.exists() && directory.isDirectory()) {
            latestDirectory = Arrays.stream(directory.listFiles(File::isDirectory))
                    .max(Comparator.comparingLong(File::lastModified))
                    .orElse(null);

            if (latestDirectory != null) {
                System.out.println("Latest Directory: " + latestDirectory.getName());
            } else {
                System.out.println("No directories found.");
                System.exit(0);
            }
        }
        storage_Folder = latestDirectory;

        List<Thread> threads = new ArrayList<>();

        Instant benchmarkStart = Instant.now();

        int totalRatio = writeRatio + readRatio;
        int numberOfWriteThreads = (numberOfThreads * writeRatio) / totalRatio;
        int numberOfReadThreads = numberOfThreads - numberOfWriteThreads;

        // Write the header to the CSV files before starting the threads
        try (FileWriter fileWriter = new FileWriter(csvFileNameMetrics, true);
             PrintWriter printWriter = new PrintWriter(fileWriter)) {
            if (new File(csvFileNameMetrics).length() == 0) { // checks if file is empty
                printWriter.println("Timestamp,Operation,Latency,DataPoints");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        // Write the header to the CSV files before starting the threads
        try (FileWriter fileWriter = new FileWriter(csvFileNamePerformance, true);
             PrintWriter printWriter = new PrintWriter(fileWriter)) {
            if (new File(csvFileNamePerformance).length() == 0) { // checks if file is empty
                printWriter.println("Timestamp,Storage in kb, CPU load, Free RAM, Total RAM");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        System.out.println("Logging results in " + csvFileNameMetrics + " ; " + csvFileNamePerformance);

        // background thread for performance
        Thread performanceLoggerThread = new Thread(() -> {
            while (!Thread.currentThread().isInterrupted()) {
                logToCSVPerformance();
                try {
                    Thread.sleep(10);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        });
        performanceLoggerThread.start();

        // Start write threads
        for (int i = 0; i < numberOfWriteThreads; i++) {
            Thread thread = new Thread(new RandomRequestsInfluxDB(true));
            threads.add(thread);
            thread.start();
        }

        // Start read threads
        for (int i = 0; i < numberOfReadThreads; i++) {
            Thread thread = new Thread(new RandomRequestsInfluxDB(false));
            threads.add(thread);
            thread.start();
        }

        for (Thread thread : threads) {
            try {
                thread.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        performanceLoggerThread.interrupt();
        try {
            performanceLoggerThread.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        Instant benchmarkEnd = Instant.now();
        Duration benchmarkDuration = Duration.between(benchmarkStart, benchmarkEnd);

        System.out.println("Number of Threads: " + numberOfThreads);
        System.out.println("Number of Requests: " + numberOfRequests);
        System.out.println("Duration of Benchmark: " + benchmarkDuration.toMillis() + " ms");

        System.out.println("Current working directory in Java: " + System.getProperty("user.dir"));


        try {
            //Runtime r = Runtime.getRuntime();
            //Process p = r.exec("python src/main/java/InfluxDB/pythonServer.py");
            //Process process = Runtime.getRuntime().exec("python src/main/java/InfluxDB/pythonServer.py");

            String command = "cmd /c start python src/main/java/InfluxDB/pythonServer.py";
            Process p = Runtime.getRuntime().exec(command);

            System.out.println("Python-Server wurde gestartet.");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }

    @Override
    public void run() {


        try (InfluxDBClient client = InfluxDBClientFactory.create(host_url, token.toCharArray(), org, bucket)) {
            Random random = new Random();

            if (isWriteOperation) {
                for (int i = 0; i < numberOfRequests; i++) {
                        Instant start = Instant.now();

                        Point point = Point.measurement("simple_measurement")
                                .addField("value", random.nextInt(1000000))
                                .time(Instant.now(), WritePrecision.NS);

                        client.getWriteApiBlocking().writePoint(bucket, org, point);

                        Instant end = Instant.now();
                        Duration latency = Duration.between(start, end);


                        logToCSVMetrics("Write", latency.toMillis(), 1);
                }
            }
            else {
                for (int i = 0; i < numberOfRequests; i++) {
                    if(timeRangeMinutesEnd == 0){
                        Instant start = Instant.now();

                        Instant rangeStartTime = Instant.now().minus(Duration.ofMinutes(timeRangeMinutesStart));

                        /*
                        String flux = String.format(
                                "from(bucket: \"%s\") " +
                                        "|> range(start: %s) " +  // Zeitfilter
                                        "|> filter(fn: (r) => r._measurement == \"simple_measurement\") " +
                                        "|> mean(column: \"_value\") " +  // Aggregation (Durchschnitt) SUM(), MAX() und MIN() auch möglich
                                        "|> yield(name: \"mean\")",
                                bucket, rangeStartTime);
                         */

                        // Current problem: Aggregations are possible
                        // However, a second API request would be necessary to query all existing data points
                        // With aggregation, only the count of the found data points is provided > Sum, Max, Min, etc. are all 1


                        String flux = String.format(
                                "from(bucket: \"%s\")" +
                                        "|> range(start: %s) " +
                                        "|> filter(fn: (r) => r._measurement == \"simple_measurement\") " +
                                        "|> keep(columns: [\"_time\", \"_value\"]) " +
                                        "|> sort() " +
                                        "|> yield(name: \"sort\")",
                                bucket, rangeStartTime);


                        List<FluxTable> tables = client.getQueryApi().query(flux, org);
                        Instant end = Instant.now();
                        Duration latency = Duration.between(start, end);
                        int dataPoints = tables.stream().mapToInt(table -> table.getRecords().size()).sum();

                        logToCSVMetrics("Read",  latency.toMillis(), dataPoints);
                    }
                    else{
                        Instant start = Instant.now();

                        Instant rangeStartTime = Instant.now().minus(Duration.ofMinutes(timeRangeMinutesStart));
                        Instant rangeEndTime = Instant.now().minus(Duration.ofMinutes(timeRangeMinutesEnd));

                        String flux = String.format(
                                "from(bucket: \"%s\") " +
                                        "|> range(start: %s, stop: %s) " +  // Zeitbereich mit Start- und Endzeit
                                        "|> filter(fn: (r) => r._measurement == \"simple_measurement\") " +
                                        "|> mean(column: \"_value\") " +  // Aggregation (Durchschnitt)
                                        "|> yield(name: \"mean\")",
                                bucket, rangeStartTime, rangeEndTime);


                        List<FluxTable> tables = client.getQueryApi().query(flux, org);
                        Instant end = Instant.now();
                        Duration latency = Duration.between(start, end);
                        int dataPoints = tables.stream().mapToInt(table -> table.getRecords().size()).sum();

                        logToCSVMetrics("Read",  latency.toMillis(), dataPoints);
                    }


                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
