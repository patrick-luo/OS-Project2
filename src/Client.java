import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;

public class Client extends AbstractDude {
	String type;
	String[] content;
	
	public Client(Socket s, String type, String[] content) throws IOException {
		super(s);
		this.type = type;
		this.content = content;
	}

	public void run() {
		try {
			IPPort ns = getNameServer();
			IPPort gs = getGoogleServer(ns);
			Message reply = doRequest(gs);
			process(reply);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	private Message doRequest(IPPort gs) throws IOException {
		Debug.println("Send " + type + " request to google, waiting...");
		initIO(new Socket(gs.ip, gs.port));
		send(generateMsg(type, content));
		return receive();
	}

	private IPPort getGoogleServer(IPPort ns) throws IOException {
		initIO(new Socket(ns.ip, ns.port));
		send(generateMsg("query", "google"));
		IPPort gs = (IPPort)receive().content;
		closeSocket();
		return gs;
	}

	private IPPort getNameServer() throws UnknownHostException, IOException {
		// TODO Auto-generated method stub
		BufferedReader reader = new BufferedReader
				(new InputStreamReader(new FileInputStream("Announcement.txt"), "UTF-8"));
		IPPort pair = new IPPort();
		pair.ip = InetAddress.getByName(reader.readLine());
	//	pair.ip = InetAddress.getByName("localhost");
		pair.port = Integer.parseInt(reader.readLine());
		reader.close();
		return pair;
	}

	@Override
	public Message generateMsg(String type, Object content) {
		Message m = new Message();
		m.type = type;
		m.content = content;
		return m;
	}

	@Override
	public Object process(Message msg) {
		Debug.println("Get result from google");
		if (msg.type.equals("indexing"))
			Debug.println((String) msg.content);
		else if (msg.type.equals("searching")) {
			String[] result = (String[]) msg.content;
			if (result == null || result.length == 0)
				Debug.println("No available book for your keywords.");
			for (int i = 0; i < result.length; i ++) 
				Debug.println("#" + (i + 1) + ": " + result[i]);
		}
		else {
			Debug.println((String) msg.content);
		}
		return null;
	}
	
	public static void main(String[] args) throws IOException {
		if (args.length < 2) {
			System.err.println("Client usage: <type:indexing|searching> <content>");
			System.exit(0);
		}
		String[] keyList = new String[args.length - 1];
		for (int i = 0; i < keyList.length; i ++)
			keyList[i] = args[i + 1];
		Client c = new Client(null, args[0], keyList);
		c.start();
	}

}
