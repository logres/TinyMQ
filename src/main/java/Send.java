import tech.logres.tinymq.endpoint.EndPoint;

public class Send {
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
        while(true){
            System.out.println("Send");
            System.out.println(endPoint.publish("hello","Hello World"));
            Thread.sleep(5000);
        }

    }
}
