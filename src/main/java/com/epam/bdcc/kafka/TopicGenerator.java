package com.epam.bdcc.kafka;

import com.epam.bdcc.utils.GlobalConstants;
import com.epam.bdcc.utils.PropertiesLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class TopicGenerator implements GlobalConstants {
    private static final Logger LOGGER = LoggerFactory.getLogger(TopicGenerator.class);

    public static void main(String[] args) {
        // load a properties file from class path, inside static method
        Properties applicationProperties = PropertiesLoader.getGlobalProperties();
        if (!applicationProperties.isEmpty()) {
            final boolean skipHeader = Boolean
                    .parseBoolean(applicationProperties.getProperty(GENERATOR_SKIP_HEADER_CONFIG));
            final long batchSleep = Long.parseLong(applicationProperties.getProperty(GENERATOR_BATCH_SLEEP_CONFIG));
            final int batchSize = Integer.parseInt(applicationProperties.getProperty(BATCH_SIZE_CONFIG));
            final String sampleFile = applicationProperties.getProperty(GENERATOR_SAMPLE_FILE_CONFIG);
            final String topicName = applicationProperties.getProperty(KAFKA_RAW_TOPIC_CONFIG);

            //TODO : Read the file (one_device_2015-2017.csv) and push records to Kafka raw topic.

            throw new UnsupportedOperationException("Read the file (one_device_2015-2017.csv) and push records to Kafka raw topic.");
        }
    }
}
