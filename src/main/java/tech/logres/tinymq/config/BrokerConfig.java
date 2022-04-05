package tech.logres.tinymq.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;
import org.springframework.stereotype.Component;

@Component
@Configuration
@ConfigurationProperties(prefix="broker")
public class BrokerConfig {

    public String port;

    BrokerConfig(){

    }

    public void setPort(String port) {
        this.port = port;
    }
}