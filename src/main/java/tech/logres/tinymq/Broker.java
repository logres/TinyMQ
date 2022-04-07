package tech.logres.tinymq;


import tech.logres.tinymq.config.GlobalConfig;

import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

/**
 * Broker 负责topic与queue
 */
public class Broker {

    Map<String, List<String>> keyToName = new ConcurrentHashMap<>();                //键值对应的队列名列表
    Map<String, BlockingQueue<String>> nameToQueue = new ConcurrentHashMap<>();     //队列名对应的队列

    public void start() {

        try {
            ServerSocket server = new ServerSocket(8888);
            System.out.println("Waiting for connect");

            ExecutorService executor;
            switch (GlobalConfig.THREAD_MODE) {  //选择线程池模式
                case "FIXED":
                    executor = Executors.newFixedThreadPool(GlobalConfig.THREAD_NUM);
                    break;
                case "CACHED":
                    executor = Executors.newCachedThreadPool();
                    break;
                default:
                    return;
            }
            while (true) {
                Socket client = server.accept();
                executor.execute(new ConnectHandler(client, this));     //子线程，获取消息，调用处理方法
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }


    /**
     *队列建立与配置
     * @param func 命令类型
     * @param queueName 队列名
     * @param keyList 相关key值
     * @return 成功 true ; 失败 false
     */
    public boolean declare(String func, String queueName, List<String> keyList) {   //声明处理
        switch (func) {
            case "ADD":     //添加队列键值
                for (String key : keyList) {
                    System.out.println(key);
                    if (!keyToName.containsKey(key)) {
                        keyToName.put(key, new ArrayList<>());
                    }
                    keyToName.get(key).add(queueName);
                }
                break;
            case "OPEN":    //新建队列
                nameToQueue.put(queueName, new LinkedBlockingQueue<>());
                for (String key : keyList) {
                    if (!keyToName.containsKey(key)) {
                        keyToName.put(key, new ArrayList<>());
                    }
                    keyToName.get(key).add(queueName);
                }
                break;
            case "DELETE":     //删除队列键值
                for (String key : keyToName.keySet()) {
                    if (keyToName.get(key).contains(queueName)) {
                        keyToName.get(key).remove(queueName);
                        if (keyToName.get(key).isEmpty()) {
                            keyToName.remove(key);
                        }
                    }
                }
                break;
            case "CLOSE":  //删除队列
                for (String key : keyToName.keySet()) {
                    if (keyToName.get(key).contains(queueName)) {
                        keyToName.get(key).remove(queueName);
                        if (keyToName.get(key).isEmpty()) {
                            keyToName.remove(key);
                        }
                    }
                }
                nameToQueue.remove(queueName);
                break;
            default:
                System.out.println("default");
        }
        return true;
    }

    /**
     * 向key关联队列中插入消息
     *
     * @param key     路由键值
     * @param message 消息
     * @return 发布成功与否
     */
    public boolean publish(String key, String message) {   //发布处理

        List<String> nameList = keyToName.get(key);
        if (nameList == null) return false;
        for (String name : nameList) {
            BlockingQueue<String> queue = nameToQueue.get(name);
            try {
                queue.put(message);//put阻塞方法
            } catch (InterruptedException e) {//线程在排队中被中断
                e.printStackTrace();
            }
        }
        return true;
    }

    /**
     * 从队列中提取消息
     *
     * @param queueName 队列名
     * @return 消息;Server Error;Null Error
     */
    public String getMessage(String queueName) {
        String res;
        BlockingQueue<String> queue = nameToQueue.get(queueName);
        if (queue != null)
            try {
                res = queue.take();
            } catch (InterruptedException e) {
                e.printStackTrace();
                res = "Server Error";
            }
        else
            res = "Null Error";
        return res;
    }
}
