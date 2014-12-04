import java.io.IOException;


public interface Receiver {
	public Message receive() throws IOException;
	public Object process(Message msg);
}
