package tech.logres.tinymq.endpoint;

public interface CallBackHandler extends Runnable {     //处理函数接口，满足线程执行和消息传递
    public void setMessage(String message);
}
