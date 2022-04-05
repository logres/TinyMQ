package tech.logres.tinymq.endpoint;

import java.io.*;
import java.net.Socket;
import java.nio.charset.StandardCharsets;

public class EndPoint {

    String IP;
    int port;

    public EndPoint(String IP, int port){
        this.IP=IP;
        this.port=port;
    }

    private String getString(Socket sock, BufferedWriter writer, StringBuilder message) throws IOException {
        writer.write(String.valueOf(message));
        writer.flush();

        InputStream input = sock.getInputStream();
        var reader = new BufferedReader(new InputStreamReader(input, StandardCharsets.UTF_8));

        String res = reader.readLine();
        return String.valueOf(res);
    }

    public String basicGet(String userName){
        try (Socket sock = new Socket(this.IP,this.port);){
            OutputStream output = sock.getOutputStream();
            var writer = new BufferedWriter(new OutputStreamWriter(output, StandardCharsets.UTF_8));
            StringBuilder message = new StringBuilder();

            message.append("Get").append("::").append(userName).append("\r\n");

            return getString(sock, writer, message);
        } catch (IOException e) {
            e.printStackTrace();
            return "Local Error";
        }

    }

    public String topicDeclare(String topic){
        try (Socket sock = new Socket(this.IP,this.port);){

            OutputStream output = sock.getOutputStream();
            var writer = new BufferedWriter(new OutputStreamWriter(output, StandardCharsets.UTF_8));
            StringBuilder message = new StringBuilder();

            message.append("Declare").append("::").append(topic).append("\r\n");

            return getString(sock, writer, message);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return "Local Error";
    }

    public String publish(String topic, String origin){
        try (Socket sock = new Socket(this.IP,this.port);){

            OutputStream output = sock.getOutputStream();
            var writer = new BufferedWriter(new OutputStreamWriter(output, StandardCharsets.UTF_8));
            StringBuilder message = new StringBuilder();

            message.append("Publish").append("::").append(topic).append("::").append(origin).append("\r\n");

            return getString(sock, writer, message);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return "Local Error";
    }

    public String subscribe(String topic,String userName){
        try (Socket sock = new Socket(this.IP,this.port);){

            OutputStream output = sock.getOutputStream();
            var writer = new BufferedWriter(new OutputStreamWriter(output, StandardCharsets.UTF_8));
            StringBuilder message = new StringBuilder();

            message.append("Subscribe").append("::").append(topic).append("::").append(userName).append("\r\n");

            return getString(sock, writer, message);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return "Local Error";
    }
}
