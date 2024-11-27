package InfluxDB;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Scanner;

public class BenchmarkInfluxDB {
    private static final Properties properties = new Properties();

    static {
        try (InputStream input = BenchmarkInfluxDB.class.getClassLoader().getResourceAsStream("config.properties")) {
            if (input == null) {
                System.out.println("Sorry, unable to find configBench.properties");
            } else {
                properties.load(input);
            }
        } catch (IOException ex) {
            ex.printStackTrace();
        }
    }

    public static void main(String[] args) {
        Map<String, String> config = new HashMap<>();

        // commando line values
        for (String arg : args) {
            int delimiterIndex = arg.indexOf("=");
            if (delimiterIndex != -1) {
                String key = arg.substring(0, delimiterIndex);
                String value = arg.substring(delimiterIndex + 1);
                config.put(key, value);
            }
        }

        config.putIfAbsent("NUMBER_THREADS", properties.getProperty("NUMBER_THREADS"));
        config.putIfAbsent("NUMBER_REQUESTS_PER_THREAD", properties.getProperty("NUMBER_REQUESTS_PER_THREAD"));
        config.putIfAbsent("NUMBER_OF_DATASETS", properties.getProperty("NUMBER_OF_DATASETS"));
        config.putIfAbsent("INIT", properties.getProperty("INIT"));
        config.putIfAbsent("WRITE_RATIO", properties.getProperty("WRITE_RATIO"));
        config.putIfAbsent("READ_RATIO", properties.getProperty("READ_RATIO"));
        config.putIfAbsent("TIME_RANGE_START_MINUTES", properties.getProperty("TIME_RANGE_START_MINUTES"));
        config.putIfAbsent("TIME_RANGE_END_MINUTES", properties.getProperty("TIME_RANGE_END_MINUTES"));
        config.putIfAbsent("INFLUX_DATA_PATH", properties.getProperty("INFLUX_DATA_PATH"));
        config.putIfAbsent("INFLUX_TOKEN", properties.getProperty("INFLUX_TOKEN"));
        config.putIfAbsent("ORG_ID", properties.getProperty("ORG_ID"));
        config.putIfAbsent("INFLUX_ORG", properties.getProperty("INFLUX_ORG"));
        config.putIfAbsent("HOST_URL", properties.getProperty("HOST_URL"));

        // check if all params are set
        String[] requiredParams = {"NUMBER_THREADS", "NUMBER_REQUESTS_PER_THREAD", "NUMBER_OF_DATASETS", "INIT", "WRITE_RATIO", "READ_RATIO", "TIME_RANGE_START_MINUTES", "TIME_RANGE_END_MINUTES", "INFLUX_DATA_PATH", "INFLUX_TOKEN", "ORG_ID", "INFLUX_ORG", "HOST_URL"};
        for (String param : requiredParams) {
            if (config.get(param) == null) {
                System.out.println("Error: Missing required parameter: " + param);
                System.exit(1);
            }
        }


        int numberThreads = Integer.parseInt(config.get("NUMBER_THREADS"));
        int numberRequestsPerThread = Integer.parseInt(config.get("NUMBER_REQUESTS_PER_THREAD"));
        int numberOfDatasets = Integer.parseInt(config.get("NUMBER_OF_DATASETS"));
        boolean init = Boolean.parseBoolean(config.get("INIT"));
        int writeRatio = Integer.parseInt(config.get("WRITE_RATIO"));
        int readRatio = Integer.parseInt(config.get("READ_RATIO"));
        int timeRangeMinutesStart = Integer.parseInt(config.get("TIME_RANGE_START_MINUTES"));
        int timeRangeMinutesEnd = Integer.parseInt(config.get("TIME_RANGE_END_MINUTES"));
        String influxDataPath = config.get("INFLUX_DATA_PATH");
        String influx_token = config.get("INFLUX_TOKEN");
        String organisation_id = config.get("ORG_ID");
        String influx_org = config.get("INFLUX_ORG");
        String host_url = config.get("HOST_URL");

        String bucket = null;

        if (init) {
            bucket = InitializeDB.initializeDatabase(influx_org, influx_token, numberOfDatasets, organisation_id);
        }

        System.out.println("Running on: " + host_url);
        System.out.println("Running with:");
        System.out.println("Influx Token: " + influx_token);
        System.out.println("Influx Org: " + influx_org);
        System.out.println("Influx OrgID: " + organisation_id);
        System.out.println("Number of threads: " + numberThreads);
        System.out.println("Number of Requests per thread: " + numberRequestsPerThread);
        System.out.println("Write-Ratio: " + writeRatio);
        System.out.println("Read-Ratio: " + readRatio);
        System.out.println("Time-Range-Minutes-Start: " + timeRangeMinutesStart);
        System.out.println("Time-Range-Minutes-End: " + timeRangeMinutesStart);
        System.out.println("Influx-Data-Path: " + influxDataPath);

        if (bucket == null) {
            Scanner scanner = new Scanner(System.in);
            System.out.println("Enter Bucket name:");
            bucket = scanner.nextLine();
        }

        System.out.println("Starting with: " + bucket);

        // send requests
        RandomRequestsInfluxDB.sendRandomRequests(bucket, influx_org, influx_token, numberRequestsPerThread, numberThreads, host_url, writeRatio, readRatio, timeRangeMinutesStart, timeRangeMinutesEnd, influxDataPath);
    }
}
