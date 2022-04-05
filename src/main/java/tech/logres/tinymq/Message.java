package tech.logres.tinymq;

public class Message {
    String topicName;
    String content;

    Message(String topicName,String content){
        this.topicName=topicName;
        this.content=content;
    }
}
