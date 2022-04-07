package tech.logres.tinymq.endpoint;
import java.io.*;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * 客户端类，用于在客户端进行消息的收发，与服务端共同完成MQ任务
 */
public class EndPoint {

    protected Queue<String> key = new ConcurrentLinkedQueue<>();    //异步发布队列
    protected Queue<String> message = new ConcurrentLinkedQueue<>();

    String IP;
    int port;

    /**
     * 给定服务端地址，建立连接
     * @param IP IP
     * @param port 端口号
     */
    public EndPoint(String IP, int port){
        this.IP=IP;
        this.port=port;

        //启动相关维护线程
        new Thread(new EndHandler("publish", this)).start();
    }

    /**
     * 给定套接字，输入接口与消息，等待回复
     * @param sock
     * @param message
     * @return
     * @throws IOException
     */
    protected String getString(Socket sock, StringBuilder message) throws IOException {
        OutputStream output = sock.getOutputStream();
        var writer = new BufferedWriter(new OutputStreamWriter(output, StandardCharsets.UTF_8));
        writer.write(String.valueOf(message));
        writer.flush();

        InputStream input = sock.getInputStream();
        var reader = new BufferedReader(new InputStreamReader(input, StandardCharsets.UTF_8));

        String res = reader.readLine();
        return String.valueOf(res);
    }

    /**
     * 发布消息
     * @param key 路由键值
     * @param message 消息体
     */
    public void publish(String key, String message){     //消息发布
        this.key.offer(key);
        this.message.offer(message);
    }

    /**
     * 订阅消息，给定订阅的消息队列，以及回调函数
     * @param queueName
     * @param handler
     */
    public void subscribe(String queueName, CallBackHandler handler){     //消息订阅
        try (Socket sock = new Socket(this.IP,this.port);){


            StringBuilder message = new StringBuilder();
            message.append("Subscribe").append("::").append(queueName).append("\r\n");

            String mes = getString(sock, message);

            if(mes.compareTo("ERROR") != 0){   //顺利订阅，执行处理函数
                handler.setMessage(mes);
                new Thread(handler).start();
            }
            else {
                System.out.println("Server Error.");
            }
        } catch (IOException e) {
            e.printStackTrace();
            System.out.println("Connect Error.");
        }
    }

    private void declare(String func, String queueName, List<String> keys) {     //消息订阅
        try (Socket sock = new Socket(this.IP,this.port);){

            StringBuilder message = new StringBuilder();
            message.append("Declare").append("::").append(func).append("::").append(queueName).append("::");
            if (keys != null && keys.size() > 0){
                for (int i = 0; i < keys.size(); i++){
                    if(i >= 1) message.append(":");
                    message.append(keys.get(i));
                }
            }
            else {
                message.append("NULL");
            }
            message.append("\r\n");

            String mes = getString(sock, message);
            if(mes.compareTo("ERROR") == 0) {
                System.out.println("Server Error.");
            }
        } catch (IOException e) {
            e.printStackTrace();
            System.out.println("Connect Error.");
        }
    }

    public void openQueue(String queueName){     //新增队列
        declare("OPEN", queueName, null);
    }

    public void addKey(String queueName, List<String> keys){     //添加键值
        declare("ADD", queueName, keys);
    }

    public void deleteKey(String queueName, List<String> keys){     //删除键值
        declare("DELETE", queueName, keys);
    }

    public void closeQueue(String queueName){     //关闭队列
        declare("CLOSE", queueName, null);
    }
}
