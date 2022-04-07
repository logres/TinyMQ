package tech.logres.tinymq.endpoint.handler;

import tech.logres.tinymq.config.GlobalConfig;
import tech.logres.tinymq.endpoint.EndPoint;

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
            while (!endPoint.key.isEmpty()){

                String key = endPoint.key.poll();
                String message = endPoint.message.poll();

                String flag = "ERROR";
                int times = 0;
                while (flag.compareTo("ERROR") == 0 && times < GlobalConfig.CONNECT_TIMES){

                    try (Socket sock = new Socket(endPoint.IP,endPoint.port);){

                        OutputStream output = sock.getOutputStream();
                        var writer = new BufferedWriter(new OutputStreamWriter(output, StandardCharsets.UTF_8));
                        StringBuilder mes = new StringBuilder();

                        mes.append("Publish").append("::").append(key).append("::").append(message).append("\r\n");

                        flag = endPoint.getString(sock, writer, mes);
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
