package com.epam.bdcc.kafka;

import com.epam.bdcc.htm.MonitoringRecord;
import org.apache.kafka.clients.producer.internals.DefaultPartitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.kafka.common.utils.*;

import java.util.List;
import java.util.Map;

public class MonitoringRecordPartitioner extends DefaultPartitioner {
    private static final Logger LOGGER = LoggerFactory.getLogger(MonitoringRecordPartitioner.class);
    private String monitoringRecord;

    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        if (value instanceof MonitoringRecord) {
            List<PartitionInfo> partitions = cluster.partitionsForTopic(topic);
            int numPartitions = partitions.size();
            int sp = (int) Math.abs(numPartitions * 0.3);
            int p = 0;

            if ((keyBytes == null) || (!(key instanceof String)))
                try {
                    throw new Exception("All messages must have statecode, countrycode, poc,sitenum ,parameterCode as key");
                } catch (Exception e) {
                    e.printStackTrace();
                }

            if (((String) key).equals(monitoringRecord))
                p = Utils.abs(Utils.murmur2(valueBytes)) % sp;
            else
                p = Utils.abs(Utils.murmur2(keyBytes)) % (numPartitions - sp) + sp;

            return p;
        } else {
            return super.partition(topic, key, keyBytes, value, valueBytes, cluster);
        }
    }

    public void close() {
    }

    public void configure(Map<String, ?> map) {
        monitoringRecord = map.get("10-001-0002-44201-1").toString();
    }
}