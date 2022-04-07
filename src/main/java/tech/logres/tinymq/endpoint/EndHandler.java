package tech.logres.tinymq.endpoint;

import tech.logres.tinymq.config.GlobalConfig;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.net.Socket;
import java.nio.charset.StandardCharsets;

public class EndHandler implements CallBackHandler{    //客户端服务用线程

    private String mode;
    private EndPoint endPoint;

    private String message;

    public EndHandler(String mode, EndPoint endPoint){
        this.mode = mode;
        this.endPoint = endPoint;
    }

    @Override
    public void setMessage(String message) {
        this.message = message;
    }

    @Override
    public void run() {
        switch (mode){
            case "publish":
                publish();
                break;
            case "subscribe":
                subscribe();
                break;
        }
    }

    private void publish(){     //异步发布消息
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

                        flag = endPoint.getString(sock, mes);
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

    private void subscribe(){   //订阅回调方法
        //TODO
        System.out.println(message);
    }

}
