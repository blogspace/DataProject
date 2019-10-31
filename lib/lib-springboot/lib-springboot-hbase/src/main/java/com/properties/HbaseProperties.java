package com.properties;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Primary;
@Getter
@Setter
@Primary
@ConfigurationProperties(prefix = "hbase")
public class HbaseProperties {
    private String quorum;
    private String clientPort;
    private String znode;

}
