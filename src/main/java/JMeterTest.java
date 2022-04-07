import tech.logres.tinymq.endpoint.EndPoint;

import java.util.ArrayList;
import java.util.List;

public class JMeterTest {           //该入口用于对JMeter并发测试设置队列及键值
    static String IP = "localhost";
    static int port = 8888;

    public static void main(String[] argv) throws Exception {
        /**
         * 1. 工厂方法、设置IP
         * 2. 创建信道，指定队列名称
         * 3. 发送消息
         */
        EndPoint endPoint = new EndPoint(IP,port);
        endPoint.openQueue("newQueue");
        List<String> keys = new ArrayList<>();
        keys.add("Hello");
        endPoint.addKey("newQueue", keys);
    }
}
