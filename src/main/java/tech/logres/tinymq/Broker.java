package tech.logres.tinymq;


import org.springframework.beans.factory.annotation.Autowired;
import tech.logres.tinymq.config.BrokerConfig;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Broker 负责topic与queue
 */
public class Broker {

    @Autowired
    BrokerConfig brokerConfig;

    //消息队列 按照Topic-队列 进行存储
    Map<String, Queue<String>> message = new ConcurrentHashMap<>();

    //订阅表，按照Subscriber-Topic进行存储
    Map<String, Queue<String>> subscribes = new ConcurrentHashMap<>();

    public void start(){
        System.out.println(brokerConfig==null);

        try {
            ServerSocket server = new ServerSocket( 8888);
            System.out.println("Waiting for connect");
            while(true){
                Socket client = server.accept();
                new Thread(new ConnectHandler(client,this)).start();
            }

        }catch (Exception ex){
            ex.printStackTrace();
        }
    }

    /**
     * 处理订阅事务
     * @param subscriber
     * @param topic
     */
    public int subscribe(String subscriber,String topic){
        //新用户，增加id
        if(!this.subscribes.containsKey(subscriber)) {
            this.subscribes.put(subscriber, new ConcurrentLinkedQueue<>());
        }
        //非重复订阅
        if(!this.subscribes.get(subscriber).contains(topic)){
            this.subscribes.get(subscriber).add(topic);
            return 0;
        }
        return 1;

    }

    /**
     * 处理发布事务
     * @param topic
     * @param message
     */
    public int publish(String topic,String message){
        if(this.message.containsKey(topic)){
            this.message.get(topic).add(message);
            return 0;
        }else {
            return 1;
        }
    }

    /**
     * 处理声明事务
     */
    public int declare(String topic){
        if(!this.message.containsKey(topic)){
            this.message.put(topic,new ConcurrentLinkedQueue<>());
            return 0;
        }
        return 1;
    }

    /**
     * 处理拉取业务
     * @param subscriber
     * @return
     */
    public String get(String subscriber){
        String result="";
        Queue<String> topicSet=this.subscribes.get(subscriber);
        for (String s : topicSet){
            if(!this.message.get(s).isEmpty()){
                result = this.message.get(s).poll();
                break;
            }
        }
        return result;
    }
}
