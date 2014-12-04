import java.io.IOException;


public interface Sender {
	public Message generateMsg(String type, Object content);
	public void send(Message msg) throws IOException;
}
