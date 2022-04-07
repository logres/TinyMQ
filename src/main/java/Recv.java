import tech.logres.tinymq.endpoint.EndPoint;
import tech.logres.tinymq.endpoint.handler.CallBackHandler;

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

        test(1, endPoint);
    }

    private static void test(int mode, EndPoint endPoint){
        switch (mode){
            case 1:
                endPoint.subscribe("newQueue", new CallBackHandler());
                try {Thread.sleep(5000);} catch (InterruptedException e) {e.printStackTrace();}
                endPoint.unSubscribe("newQueue");
                break;

            case 2:
                while(true){
                    System.out.println(endPoint.get("newQueue"));
                    try {Thread.sleep(1000);} catch (InterruptedException e) {e.printStackTrace();}
                }
        }
    }

}
