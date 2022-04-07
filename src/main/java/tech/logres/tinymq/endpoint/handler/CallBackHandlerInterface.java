package tech.logres.tinymq.endpoint.handler;

public interface CallBackHandlerInterface extends Runnable {     //回调函数接口，满足线程执行和消息传递
    public void setMessage(String message);
}
