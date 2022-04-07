package tech.logres.tinymq;

import org.springframework.stereotype.Component;

/**
 * TinyMQ class enable a cluster of broker, and at least one broker is started here.
 * For reason of making it simple enough, it just start a broker instance instead of manage a namelist of multiple
 * brokers.
 */
@Component
public class TinyMQ {

    public void start(){
        new Broker().start();
    }
}
