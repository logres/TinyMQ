package tech.logres.tinymq.endpoint.handler;

public class CallBackHandler implements CallBackHandlerInterface{  //回调函数类

    private String message = "";

    @Override
    public void run() {
        System.out.println(message);
    }

    @Override
    public void setMessage(String message) {
        this.message = message;
    }
}
