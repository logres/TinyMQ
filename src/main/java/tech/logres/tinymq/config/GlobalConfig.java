package tech.logres.tinymq.config;

public class GlobalConfig {

    public static final int THREAD_NUM = 10000;                     //FIX线程池线程数
    //public static final String THREAD_MODE = "FIXED";                 //线程池模式
    public static final String THREAD_MODE = "CACHED";

    public static final int CONNECT_TIMES = 10;                     //publish重连次数

    public static final boolean LOG = false;                        //输出消息信息
}
