package tech.logres.tinymq;

import tech.logres.tinymq.config.GlobalConfig;

import java.io.*;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

public class ConnectHandler implements Runnable {

    Socket client;
    Broker caller;

    Map<String,String> messageMap = new ConcurrentHashMap<>();
    List<String> keyList = new ArrayList<>();

    public ConnectHandler(Socket client,Broker caller){
        this.client = client;
        this.caller = caller;
    }

    /**
     * 解析协议,将解析后的片段存入 messageMap与keyList
     * Action::
     * Declare::Func::QueueName::Key
     *          ADD
     *          OPEN
     *          DELETE
     *          CLOSE
     * Publish::Key::Message
     * Get::QueueName
     * Subscribe::QueueName
     * @param origin 原始消息
     */
    private void messageDealer(String origin){      //主协议解析
        if(origin==null) return;
        String[] firstStage = origin.split("::",4);
        messageMap.put("Action",firstStage[0]);
        switch (firstStage[0]){
            case "Declare":
                messageMap.put("Func",firstStage[1]);
                messageMap.put("QueueName",firstStage[2]);
                keyDealer(firstStage[3]);
                break;
            case "Publish":
                messageMap.put("Key",firstStage[1]);
                messageMap.put("Message",firstStage[2]);
                break;
            case "Get":
            case "Subscribe":
                messageMap.put("QueueName",firstStage[1]);
                break;
            default:
        }

    }

    /**
     * 传入key1:key2:key3……格式的key串，解析并存入keyList
     * @param origin 原始key串
     */
    private void keyDealer(String origin){      //键值解析
        if(!origin.equals("NULL")){
            String[] keys = origin.split(":");
            for(String key : keys){
                keyList.add(key);
            }
        }
    }


    /**
     * 主要方法，处理请求
     */
    @Override
    public void run() {
        while (!client.isClosed()){
            try{
                //同步io读取message
                var reader = new BufferedReader(new InputStreamReader(this.client.getInputStream(), StandardCharsets.UTF_8));
                String message = reader.readLine();

                if(GlobalConfig.LOG) System.out.println("START >> "+message+" >> END"+"\n");    //输出消息信息

                messageDealer(message);      //解析

                String res = "";
                switch (messageMap.get("Action")){      //调用
                    case "Declare":
                        if(caller.declare(messageMap.get("Func"), messageMap.get("QueueName"), keyList)){
                            res = "Success";
                        }else{
                            res = "Fail";
                        }
                        break;

                    case "Publish":
                        if(caller.publish(messageMap.get("Key"),messageMap.get("Message"))){
                            res = "Success";
                        }
                        else{
                            res = "Fail";
                        }
                        break;

                    case "Get":
                    case "Subscribe":
                        res = caller.getMessage(messageMap.get("QueueName"));
                        break;
                    default:
                        res = "Unknown Command Error";
                }
                var writer = new BufferedWriter(new OutputStreamWriter(this.client.getOutputStream(), StandardCharsets.UTF_8));
                writer.write(res+"\r\n");
                writer.flush();

            } catch (IOException e) {
                e.printStackTrace();
            }
            try{
                if(messageMap.get("Action").compareTo("Subscribe") != 0){   //subscribe下线程不终止
                    client.close();
                }
            }catch (Exception e){e.printStackTrace();}
        }
    }
}
