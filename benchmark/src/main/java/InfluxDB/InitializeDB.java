package InfluxDB;

import com.influxdb.client.InfluxDBClient;
import com.influxdb.client.InfluxDBClientFactory;
import com.influxdb.client.WriteApiBlocking;
import com.influxdb.client.domain.WritePrecision;
import com.influxdb.client.write.Point;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Instant;
import java.util.Random;
import java.util.UUID;


/**
 * Creates a new bucket and adds Data
 */
public class InitializeDB {

    /**
     * Creates a bucket and populates it with a timestamp and a random integer.
     *
     * @param org            Organization for Influx
     * @param token          Influx token
     * @param dataSize       Number of data records
     * @param org_id         Organization ID for creating the bucket
     */

    public static String initializeDatabase(String org, String token, int dataSize, String org_id) {
        String bucket = "bucket_" + UUID.randomUUID();

        createBucket(bucket, org_id, token);

        try (InfluxDBClient client = InfluxDBClientFactory.create("http://localhost:8086", token.toCharArray(), org, bucket)) {
            WriteApiBlocking writeApi = client.getWriteApiBlocking();
            Random random = new Random();

            for (int i = 0; i < dataSize; i++) {
                Point point = Point.measurement("simple_measurement")
                        .addField("value", random.nextInt(1000000))
                        .time(Instant.now(), WritePrecision.NS);

                writeApi.writePoint(bucket, org, point);

                //System.out.println("Daten geschrieben: " + point.toLineProtocol());
            }
            System.out.printf("%s Datensätze geschrieben\n", dataSize);
        } catch (Exception e) {
            e.printStackTrace();
        }
        finally {
            //System.out.println("Error while executing");
            return bucket;
        }
    }

    /**
     * Populates the given bucket with a timestamp and a random integer.
     *
     * @param bucket         Previously randomly created bucket
     * @param orgID          Organization ID
     * @param token          Influx token
     */

    private static void createBucket(String bucket, String orgID, String token) {
        try {
            URI uri = new URI("http://localhost:8086/api/v2/buckets");
            HttpClient client = HttpClient.newHttpClient();

            String jsonInputString = String.format(
                    "{ \"orgID\": \"%s\"," + "\"name\": \"%s\", \"retentionRules\": [ { \"type\": \"expire\", \"everySeconds\": 86400, \"shardGroupDurationSeconds\": 0 } ] }",
                    orgID, bucket
            );

            HttpRequest request = HttpRequest.newBuilder(uri)
                    .header("Authorization", "Token " + token)
                    .header("Content-Type", "application/json")
                    .POST(HttpRequest.BodyPublishers.ofString(jsonInputString))
                    .build();

            HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());

            if (response.statusCode() == 201) {
                System.out.println("Bucket erstellt: " + bucket);
            } else {
                System.err.println("Fehler beim Erstellen des Buckets: HTTP " + response.statusCode());
                System.err.println(response.body());
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
