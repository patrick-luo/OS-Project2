import java.io.Serializable;
import java.net.InetAddress;


public class Message implements Serializable {
	public InetAddress ip;
	public int port;
	public String type;
	public Object content;
}
