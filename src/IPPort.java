import java.io.Serializable;
import java.net.InetAddress;


public class IPPort implements Serializable {
	private static final long serialVersionUID = 1L;
	public InetAddress ip;
	public int port;
	
	@Override
	public boolean equals(Object o) {
		IPPort pair = (IPPort)o;
		if (this.ip.equals(pair.ip) && this.port == pair.port)
			return true;
		else
			return false;
	}
	
	public String toString(){
		return this.ip.toString() + ":" + port;
	}
}
