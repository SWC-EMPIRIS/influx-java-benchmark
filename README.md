# Basic Benchmark

## Setup and Usage

### Command-Line Arguments for `BenchmarkInfluxDB`

- **`NUMBER_THREADS`**: The number of threads used for concurrent data operations
- **`NUMBER_REQUESTS_PER_THREAD`**: Number of requests each thread will execute
- **`NUMBER_OF_DATASETS`**: Total number of datasets to be written in an InfluxDB bucket
- **`INIT`**: Indicates whether to initialize a new bucket (`true`/`false`) for now please only use true
- **`WRITE_RATIO`**: Ratio of write operations compared to read operations
- **`READ_RATIO`**: Ratio of read operations compared to write operations
- **`TIME_RANGE_START_MINUTES`**: Start of the time range for read operations (in minutes)
- **`TIME_RANGE_END_MINUTES`**: End of the time range for read operations (in minutes) 0 for now
- **`INFLUX_DATA_PATH`**: File path where InfluxDB stores its data (needs to be the "wal directory) e.g. C:\\Users\\<USER>\\.influxdbv2\\engine\\wal
- **`INFLUX_TOKEN`**: Authentication token for InfluxDB access
- **`ORG_ID`**: Organization ID in InfluxDB
- **`INFLUX_ORG`**: Organization name in InfluxDB
- **`HOST_URL`**: URL of the InfluxDB instance to connect to including port


### Benchmark Setup
1. Navigate to the `benchmark` directory:
    ```sh
    cd benchmark
    ```
2. Build the project using Maven:
    ```sh
    mvn clean install
    ```
3. Run the Benchmark application:
    ```sh
    mvn exec:java -Dexec.mainClass="InfluxDB.BenchmarkInfluxDB" -Dexec.args="NUMBER_THREADS= NUMBER_REQUESTS_PER_THREAD= NUMBER_OF_DATASETS= INIT=true WRITE_RATIO= READ_RATIO= TIME_RANGE_START_MINUTES= TIME_RANGE_END_MINUTES= INFLUX_DATA_PATH= INFLUX_TOKEN= ORG_ID= INFLUX_ORG= HOST_URL="
    ```
