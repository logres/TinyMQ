package tech.logres.tinymq;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import tech.logres.tinymq.config.NameServerConfig;

/**
 * TinyMQ class enable a cluster of broker, and at least one broker is started here.
 * For reason of making it simple enough, it just start a broker instance instead of manage a namelist of multiple
 * brokers.
 */
@Component
public class TinyMQ {

    @Autowired
    NameServerConfig nameServerConfig;

    public void start(){
        new Broker().start();
    }
}
