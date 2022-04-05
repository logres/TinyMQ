import tech.logres.tinymq.endpoint.EndPoint;

public class Recv {
    static String IP = "localhost";
    static int port = 8888;
    private final static String Topic = "hello";

    public static void main(String[] argv) throws Exception {
        /**
         * 1. 工厂方法、设置IP
         * 2. 创建信道，指定队列名称
         * 3. 发送消息
         */
        EndPoint endPoint = new EndPoint(IP,port);
        endPoint.topicDeclare(Topic);
        endPoint.subscribe(Topic,"Happy Lady");
        while(true){
            System.out.println(endPoint.basicGet("Happy Lady"));
            Thread.sleep(1000);
        }

    }
}
