import java.io.Serializable;
import java.net.InetAddress;


public class Message implements Serializable {
	private static final long serialVersionUID = 1L;
	public int xid = -1;
	public InetAddress ip;
	public int port;
	public String type;
	public Object content;
	
	public String toString(){
		return this.content.toString();
	}
}
