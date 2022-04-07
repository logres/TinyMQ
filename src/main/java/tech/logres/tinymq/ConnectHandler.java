package tech.logres.tinymq;

import tech.logres.tinymq.config.GlobalConfig;

import java.io.*;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class ConnectHandler implements Runnable {

    Socket client;
    Broker caller;

    Map<String,String> messageMap = new ConcurrentHashMap<>();
    List<String> keyList = new ArrayList<>();

    public ConnectHandler(Socket client,Broker caller){
        this.client = client;
        this.caller = caller;
    }

    private void messageDealer(String origin){      //主协议解析
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

    private void keyDealer(String origin){      //键值解析
        if(origin.compareTo("NULL") != 0){
            String[] keys = origin.split(":");
            for(int i = 0; i < keys.length; i++){
                keyList.add(keys[i]);
            }
        }
    }


    @Override
    public void run() {
        while (!client.isClosed()){
            try{
                //同步io读取message
                InputStream input = this.client.getInputStream();
                var reader = new BufferedReader(new InputStreamReader(input, StandardCharsets.UTF_8));
                String message = reader.readLine();

                if(GlobalConfig.LOG) System.out.println("START >> "+message+" >> END"+"\n");    //输出消息信息

                messageDealer(message.toString());      //解析

                String res = "";
                switch (messageMap.get("Action")){      //调用
                    case "Declare":
                        caller.declare(messageMap.get("Func"), messageMap.get("QueueName"), keyList);
                        res = "ACK";
                        break;

                    case "Publish":
                        if(caller.publish(messageMap.get("Key"),messageMap.get("Message")))
                            res = "ACK";
                        else
                            res = "ERROR";
                        break;

                    case "Get":
                    case "Subscribe":
                        res = caller.getMessage(messageMap.get("QueueName"));
                        break;

                    default:
                        res = "ERROR";
                }
                OutputStream output = this.client.getOutputStream();
                var writer = new BufferedWriter(new OutputStreamWriter(output, StandardCharsets.UTF_8));
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
