package tech.logres.tinymq.endpoint.handler;

import tech.logres.tinymq.config.GlobalConfig;
import tech.logres.tinymq.endpoint.EndPoint;
import tech.logres.tinymq.endpoint.Message;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.net.Socket;
import java.nio.charset.StandardCharsets;

public class PublishHandler implements Runnable{  //订阅线程类

    private EndPoint endPoint;

    public PublishHandler(EndPoint endPoint){
        this.endPoint = endPoint;
    }

    @Override
    public void run() {
        while (true){
            if (!endPoint.messageToPublish.isEmpty()){

                Message key_message = endPoint.messageToPublish.poll();
                String key = key_message.getKey();
                String message = key_message.getMessage();
                String flag = "ERROR";
                int times = 0;
                if (flag.equals("ERROR") && times < GlobalConfig.CONNECT_TIMES){

                    try (Socket sock = new Socket(endPoint.IP,endPoint.port)){

                        StringBuilder mes = new StringBuilder();

                        mes.append("Publish").append("::").append(key).append("::").append(message).append("\r\n");

                        flag = endPoint.sendAndGet(sock, mes);

                    } catch (IOException e) {
                        e.printStackTrace();
                        times++;
                        try{Thread.sleep(100);} catch (InterruptedException e1) {e1.printStackTrace();}
                    }

                }
                if(times == GlobalConfig.CONNECT_TIMES){
                    System.out.println("Fail to publish.\n");   //重连失败提示
                }
            }
            try{Thread.sleep(100);} catch (InterruptedException e1) {e1.printStackTrace();}
        }
    }
}
