package tech.logres.tinymq.endpoint;

public class Message {
    private String key;
    private String message;

    Message(String key,String message){
        this.key=key;
        this.message=message;
    }

    public String getKey() {
        return key;
    }

    public String getMessage() {
        return message;
    }
}
