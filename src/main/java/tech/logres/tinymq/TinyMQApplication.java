package tech.logres.tinymq;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;

@SpringBootApplication(exclude={DataSourceAutoConfiguration.class})
public class TinyMQApplication {

    public static void main(String[] args) {
        SpringApplication.run(TinyMQApplication.class, args);
        new TinyMQ().start();
    }

}
