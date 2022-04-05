package tech.logres.tinymq;

import java.io.*;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class ConnectHandler implements Runnable {

    Socket client;
    Broker caller;

    public ConnectHandler(Socket client,Broker caller){
        this.client = client;
        this.caller = caller;
    }

    public Map<String,String> messageDealer(String origin){
        String[] firstStage = origin.split("::",3);
        Map<String,String> res = new ConcurrentHashMap<>();
        res.put("Action",firstStage[0]);
        switch (firstStage[0]){
            case "Declare":
                res.put("Topic",firstStage[1]);
                break;
            case "Publish":
                res.put("Topic",firstStage[1]);
                res.put("Message",firstStage[2]);
                break;
            case "Subscribe":
                res.put("UserName",firstStage[2]);
                res.put("Topic",firstStage[1]);
                break;
            case "Get":
                res.put("UserName",firstStage[1]);
                break;
            default:
                return null;
        }

        return res;
    }

    @Override
    public void run() {
        try{
            //同步io读取message
            InputStream input = this.client.getInputStream();
            var reader = new BufferedReader(new InputStreamReader(input, StandardCharsets.UTF_8));
            String message = reader.readLine();

            System.out.println("START"+message+"END"+"\n");

            Map<String,String> messageMap = messageDealer(message.toString());

            String res;
            switch (messageMap.get("Action")){
                case "Declare":
                    if(caller.declare(messageMap.get("Topic"))==0)
                        res = "ACK";
                    else
                        res = "Error";
                    break;
                case "Publish":
                    if(caller.publish(messageMap.get("Topic"),messageMap.get("Message"))==0)
                        res = "ACK";
                    else
                        res = "ERROR";
                    break;
                case "Subscribe":
                    if(caller.subscribe(messageMap.get("UserName"),messageMap.get("Topic"))==0)
                        res = "ACK";
                    else
                        res = "ERROR";
                    break;
                case "Get":
                    res = caller.get(messageMap.get("UserName"));
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
    }
}
