package tech.logres.tinymq.endpoint.handler;

import tech.logres.tinymq.config.GlobalConfig;
import tech.logres.tinymq.endpoint.EndPoint;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.net.Socket;
import java.nio.charset.StandardCharsets;

public class SubscribeHandler implements Runnable{  //发布线程类

    private EndPoint endPoint;
    private String queueName;
    private CallBackHandlerInterface handler;


    /**
     *
     * @param endPoint
     * @param queueName
     * @param handler
     */
    public SubscribeHandler(EndPoint endPoint, String queueName, CallBackHandlerInterface handler){
        this.endPoint = endPoint;
        this.queueName = queueName;
        this.handler = handler;
    }

    @Override
    public void run() {
        int times = 0;
        while (times < GlobalConfig.CONNECT_TIMES&&!Thread.currentThread().isInterrupted()){

            try (Socket sock = new Socket(endPoint.IP,endPoint.port)){

                StringBuilder message = new StringBuilder();

                message.append("Subscribe").append("::").append(queueName).append("\r\n");

                while (!sock.isClosed() && !Thread.currentThread().isInterrupted()){   //持久连接，反复接受消息
                    String mes = endPoint.sendAndGet(sock, message);
                    if(!mes.equals("Server Error")&&!mes.equals("Null Error")){   //顺利订阅，执行处理函数
                        handler.setMessage(mes);
                        new Thread(handler).start();
                    }else {
                        System.out.println("Server error.");
                    }
                }
            } catch (IOException e) {
                if(times == GlobalConfig.CONNECT_TIMES){
                    System.out.println("Connect error.\n");   //重连失败提示
                    break;
                }
                times++;
                try{Thread.sleep(100);} catch (InterruptedException e1) {e1.printStackTrace();}
            }
        }
    }
}
