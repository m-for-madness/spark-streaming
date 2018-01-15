package com.epam.bdcc.spark;

import com.epam.bdcc.htm.HTMNetwork;
import com.epam.bdcc.htm.MonitoringRecord;
import com.epam.bdcc.htm.ResultState;
import com.epam.bdcc.utils.GlobalConstants;
import com.epam.bdcc.utils.PropertiesLoader;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function3;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.State;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;

import java.util.HashMap;
import java.util.Properties;

public class AnomalyDetector implements GlobalConstants {
    /**
     * TODO :
     * 1. Define Spark configuration (register serializers, if needed)
     * 2. Initialize streaming context with checkpoint directory
     * 3. Read records from kafka topic "monitoring20" (com.epam.bdcc.kafka.KafkaHelper can help)
     * 4. Organized records by key and map with HTMNetwork state for each device
     * (com.epam.bdcc.kafka.KafkaHelper.getKey - unique key for the device),
     * for detecting anomalies and updating HTMNetwork state (com.epam.bdcc.spark.AnomalyDetector.mappingFunc can help)
     * 5. Send enriched records to topic "monitoringEnriched2" for further visualization
     **/
    public static void main(String[] args) throws Exception {
        //load a properties file from class path, inside static method
        final Properties applicationProperties = PropertiesLoader.getGlobalProperties();
        if (!applicationProperties.isEmpty()) {
            final String appName = applicationProperties.getProperty(SPARK_APP_NAME_CONFIG);
            final String rawTopicName = applicationProperties.getProperty(KAFKA_RAW_TOPIC_CONFIG);
            final String enrichedTopicName = applicationProperties.getProperty(KAFKA_ENRICHED_TOPIC_CONFIG);
            final String checkpointDir = applicationProperties.getProperty(SPARK_CHECKPOINT_DIR_CONFIG);
            final Duration batchDuration = Duration.apply(Long.parseLong(applicationProperties.getProperty(SPARK_BATCH_DURATION_CONFIG)));
            final Duration checkpointInterval = Duration.apply(Long.parseLong(applicationProperties.getProperty(SPARK_CHECKPOINT_INTERVAL_CONFIG)));

            JavaStreamingContext jssc = null;
            //TODO : Create your pipeline here!!!

            jssc.start();
            jssc.awaitTermination();
        }
    }

    private static Function3<String, Optional<MonitoringRecord>, State<HTMNetwork>, MonitoringRecord> mappingFunc =
            (deviceID, recordOpt, state) -> {
                // case 0: timeout
                if (!recordOpt.isPresent())
                    return null;

                // either new or existing device
                if (!state.exists())
                    state.update(new HTMNetwork(deviceID));
                HTMNetwork htmNetwork = state.get();
                String stateDeviceID = htmNetwork.getId();
                if (!stateDeviceID.equals(deviceID))
                    throw new Exception("Wrong behaviour of Spark: stream key is $deviceID%s, while the actual state key is $stateDeviceID%s");
                MonitoringRecord record = recordOpt.get();

                // get the value of DT and Measurement and pass it to the HTM
                HashMap<String, Object> m = new java.util.HashMap<>();
                m.put("DT", DateTime.parse(record.getDateGMT() + " " + record.getTimeGMT(), DateTimeFormat.forPattern("YY-MM-dd HH:mm")));
                m.put("Measurement", Double.parseDouble(record.getSampleMeasurement()));
                ResultState rs = htmNetwork.compute(m);
                record.setPrediction(rs.getPrediction());
                record.setError(rs.getError());
                record.setAnomaly(rs.getAnomaly());
                record.setPredictionNext(rs.getPredictionNext());

                return record;
            };
}