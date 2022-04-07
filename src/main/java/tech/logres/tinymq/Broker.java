package tech.logres.tinymq;


import org.springframework.beans.factory.annotation.Autowired;
import tech.logres.tinymq.config.BrokerConfig;
import tech.logres.tinymq.config.GlobalConfig;

import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.*;

/**
 * Broker 负责topic与queue
 */
public class Broker {

    @Autowired
    BrokerConfig brokerConfig;

    Map<String, List<String>> keyToName = new ConcurrentHashMap<>();                //键值对应的队列名列表
    Map<String, BlockingQueue<String>> nameToQueue = new ConcurrentHashMap<>();     //队列名对应的队列


    /**
     * 单个Broker结点运行
     */
    public void start(){
        System.out.println(brokerConfig==null);

        try {
            ServerSocket server = new ServerSocket( 8888);
            System.out.println("Waiting for connect");

            ExecutorService executor;
            switch (GlobalConfig.THREAD_MODE){  //选择线程池模式
                case "FIX":
                    executor = Executors.newFixedThreadPool(GlobalConfig.THREAD_NUM);
                    break;
                case "CACHED":
                    executor = Executors.newCachedThreadPool();
                    break;
                default:
                    return;
            }

            while(true){
                Socket client = server.accept();
                //new Thread(new ConnectHandler(client,this)).start();
                executor.execute(new ConnectHandler(client,this));     //子线程，获取消息，调用处理方法
            }

        }catch (Exception ex){
            ex.printStackTrace();
        }
    }


    public void declare(String func, String queueName, List<String> keyList){   //声明处理

        switch (func){
            case "ADD":     //添加队列键值
            case "OPEN":    //新建队列
                if(!nameToQueue.containsKey(queueName)){
                nameToQueue.put(queueName, new LinkedBlockingQueue<>());
                }
                for (int i = 0; i < keyList.size(); i++){
                    String key = keyList.get(i);
                    if(!keyToName.containsKey(key)){
                        keyToName.put(key, new ArrayList<>());
                    }
                    keyToName.get(key).add(queueName);
                }
                break;

            case "DELETE":     //删除队列键值
                for (String key : keyToName.keySet()){
                    List<String> list = keyToName.get(key);
                    if(list.contains(queueName)){
                        list.remove(queueName);
                        if (list.size() == 0){
                            keyToName.remove(key);
                        }
                    }
                }
                break;

            case "CLOSE":  //删除队列
                for (String key : keyToName.keySet()){
                    List<String> list = keyToName.get(key);
                    if(list.contains(queueName)){
                        list.remove(queueName);
                        if (list.size() == 0){
                            keyToName.remove(key);
                        }
                    }
                }
                nameToQueue.remove(queueName);
                break;
        }
    }

    public boolean publish(String key,String message){   //发布处理

        List<String> nameList = keyToName.get(key);
        if (nameList == null) return false;
        for(String name : nameList){
            BlockingQueue<String> queue = nameToQueue.get(name);
            try {
                queue.put(message);
            }catch (Exception e) {
                e.printStackTrace();
                return false;
            }
        }
        return true;
    }

    public String subscribe(String queueName){   //订阅处理
        try {
            BlockingQueue<String> queue = nameToQueue.get(queueName);
            if(queue != null) return queue.take();
            else return "ERROR";
        }catch (Exception e) {
            e.printStackTrace();
            return "ERROR";
        }
    }

}
