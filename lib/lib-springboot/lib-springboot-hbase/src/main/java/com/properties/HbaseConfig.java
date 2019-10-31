package com.properties;

import org.apache.hadoop.hbase.HBaseConfiguration;

import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.hadoop.hbase.HbaseTemplate;

@Configuration
@EnableConfigurationProperties(HbaseProperties.class)
public class HbaseConfig {

    private final HbaseProperties properties;

    public HbaseConfig(HbaseProperties properties) {
        this.properties = properties;
    }

    public org.apache.hadoop.conf.Configuration configuration() {
        org.apache.hadoop.conf.Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", properties.getQuorum());
        conf.set("hbase.zookeeper.clientPort", properties.getClientPort());
        return conf;
    }
    @Bean
    public HbaseTemplate hbaseTemplate() {
        HbaseTemplate hbaseTemplate = new HbaseTemplate();
        hbaseTemplate.setConfiguration(configuration());
        hbaseTemplate.setAutoFlush(true);
        return hbaseTemplate;
    }


}
