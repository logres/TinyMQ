package tech.logres.tinymq.endpoint;

import io.netty.util.internal.PriorityQueue;
import org.apache.ibatis.jdbc.Null;
import tech.logres.tinymq.endpoint.handler.CallBackHandlerInterface;
import tech.logres.tinymq.endpoint.handler.PublishHandler;
import tech.logres.tinymq.endpoint.handler.SubscribeHandler;

import java.io.*;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

public class EndPoint { //客户端

    public Queue<String> key = new ConcurrentLinkedQueue<>();           //异步发布队列
    public Queue<String> message = new ConcurrentLinkedQueue<>();

    private Map<String, Thread> subscribed = new LinkedHashMap<>();     //订阅线程组

    Thread handler = new Thread(new PublishHandler( this));     //发布维护线程

    public String IP;
    public int port;

    public EndPoint(String IP, int port){
        this.IP=IP;
        this.port=port;

        //启动相关维护线程
        handler.start();
    }

    public String getString(Socket sock, BufferedWriter writer, StringBuilder message) throws IOException {
        writer.write(String.valueOf(message));
        writer.flush();

        InputStream input = sock.getInputStream();
        var reader = new BufferedReader(new InputStreamReader(input, StandardCharsets.UTF_8));

        String res = reader.readLine();
        return String.valueOf(res);
    }

    public void publish(String key, String message){     //消息发布
        this.key.offer(key);
        this.message.offer(message);
    }

    public void subscribe(String queueName, CallBackHandlerInterface handler){     //消息订阅
        Thread thread = new Thread(new SubscribeHandler(this, queueName, handler));
        if(!subscribed.containsKey(queueName)){
            subscribed.put(queueName, thread);
            thread.start();
        }
    }

    public void unSubscribe(String queueName){     //取消订阅
        Thread thread = subscribed.get(queueName);
        if (thread != null) {
            thread.interrupt();
            subscribed.remove(queueName);
        }
    }

    public String get(String queueName) {     //消息获取
        try (Socket sock = new Socket(this.IP,this.port);){

            OutputStream output = sock.getOutputStream();
            var writer = new BufferedWriter(new OutputStreamWriter(output, StandardCharsets.UTF_8));
            StringBuilder message = new StringBuilder();

            message.append("Get").append("::").append(queueName).append("\r\n");

            String mes = this.getString(sock, writer, message);
            if(mes.compareTo("ERROR") != 0){
                return mes;
            }
            else {
                System.out.println("Server error.");
                return null;
            }
        } catch (IOException e) {
            e.printStackTrace();
            System.out.println("Connect error.");
        }
        return null;
    }

    private void declare(String func, String queueName, List<String> keys) {     //消息订阅
        try (Socket sock = new Socket(this.IP,this.port);){

            OutputStream output = sock.getOutputStream();
            var writer = new BufferedWriter(new OutputStreamWriter(output, StandardCharsets.UTF_8));
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

            String mes = getString(sock, writer, message);
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
