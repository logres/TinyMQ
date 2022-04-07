import tech.logres.tinymq.endpoint.EndHandler;
import tech.logres.tinymq.endpoint.EndPoint;

import java.util.ArrayList;
import java.util.List;

public class Recv {
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
        while(true){
            endPoint.subscribe("newQueue", new EndHandler("subscribe", endPoint));
            Thread.sleep(1000);
        }

    }
}
