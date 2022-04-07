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
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class EndPoint { //客户端

    public BlockingQueue<Message> messageToPublish = new LinkedBlockingQueue<>(); //本地发布消息队列

    private Map<String, Thread> subscribed = new LinkedHashMap<>();     //用于管理线程

    Thread handler = new Thread(new PublishHandler( this));     //发布维护线程

    public String IP;
    public int port;

    public EndPoint(String IP, int port){
        this.IP=IP;
        this.port=port;

        //启动相关维护线程
        handler.start();
    }

    public String sendAndGet(Socket sock, StringBuilder message) throws IOException {

        //写
        var writer = new BufferedWriter(new OutputStreamWriter(sock.getOutputStream(), StandardCharsets.UTF_8));
        writer.write(String.valueOf(message));
        writer.flush();

        //读
        var reader = new BufferedReader(new InputStreamReader(sock.getInputStream(), StandardCharsets.UTF_8));
        String res = reader.readLine();
        return String.valueOf(res);
    }

    /**
     * 发布消息
     * @param key key值
     * @param message 消息
     * @return 发布成功 true;发布失败 false
     */
    public Boolean publish(String key, String message){     //消息发布
        try {
            this.messageToPublish.put(new Message(key,message));
        } catch (InterruptedException e) {
            e.printStackTrace();
            return false;
        }
        return true;
    }

    /**
     * 客户端订阅方法
     * @param queueName 订阅的队列名
     * @param handler 回调函数
     * @return 订阅成功：true;订阅失败 false
     */
    public Boolean subscribe(String queueName, CallBackHandlerInterface handler){     //消息订阅
        Thread thread = new Thread(new SubscribeHandler(this, queueName, handler));
        if(!subscribed.containsKey(queueName)){//检测重复订阅
            subscribed.put(queueName, thread);
            thread.start();
            return true;
        }
        return false;
    }

    /**
     * 解除订阅
     * @param queueName 解除订阅的队列名
     * @return 取消成功 true; 取消失败 false
     */
    public Boolean unSubscribe(String queueName){     //取消订阅
        Thread thread = subscribed.get(queueName);
        if (thread != null) {
            thread.interrupt();
            subscribed.remove(queueName);
            return true;
        }
        return false;
    }

    /**
     * 获取单条消息
     * @param queueName 获取的队列
     * @return 正常 消息/失败 null
     */
    public String get(String queueName) {     //消息获取
        try (Socket sock = new Socket(this.IP,this.port);){
            StringBuilder message = new StringBuilder();

            message.append("Get").append("::").append(queueName).append("\r\n");

            String mes = this.sendAndGet(sock,message);
            if(mes.equals("Null Error")) {
                System.out.println("No Such Queue");
                return null;
            }else if(mes.equals("Server Error")){
                System.out.println("Server Error");
                return null;
            }
            return mes;
        } catch (IOException e) {
            e.printStackTrace();
            System.out.println("Connect error.");
            return null;
        }
    }

    /**
     * 队列的配置
     * @param func 方法 OPEN/ADD/DELETE/CLOSE
     * @param queueName 队列名
     * @param keys key值
     */
    private void declare(String func, String queueName, List<String> keys) {     //消息订阅
        try (Socket sock = new Socket(this.IP,this.port)){
            StringBuilder message = new StringBuilder();

            message.append("Declare").append("::").append(func).append("::").append(queueName).append("::");
            if (keys != null){
                for (int i = 0; i < keys.size(); i++){
                    if(i >= 1) message.append(":");
                    message.append(keys.get(i));
                }
            }
            else {
                message.append("NULL");
            }
            message.append("\r\n");

            String mes = sendAndGet(sock, message);
            if(mes.equals("Fail")) {
                System.out.println("Declare Failed.");
            }else {
                System.out.println("Declare Success");
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
