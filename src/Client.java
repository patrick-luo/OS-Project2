import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;

public class Client implements Sender, Receiver{
	private IPPort nameServer;
	private InetAddress ip;
	private int port;
	private Socket clientSocket;
	private ObjectOutputStream writer;
	private ObjectInputStream reader;

	public Client() throws UnknownHostException, IOException {
		nameServer = getNameServer();
		clientSocket = new Socket(nameServer.ip, nameServer.port);
		writer = new ObjectOutputStream(clientSocket.getOutputStream());
		writer.flush();
		reader = new ObjectInputStream(clientSocket.getInputStream());
		ip = InetAddress.getLocalHost();
		port = clientSocket.getLocalPort();
	}

	public static void main(String[] args) throws UnknownHostException,
			IOException {
		Client client = new Client();
		Message request = client.generateMsg("query", "google");
		client.send(request);
		Debug.println("client: sends query for location of googleServer");
		String replyMsgFromNS = client.receive().toString();
		Debug.println("client: receives reply from NS " + replyMsgFromNS);
		//send request to the server
		String documentPath = "D1";
		String requestType = "indexing";
		String requestContent = documentPath;
		Message requestMsg = client.generateMsg(requestType, requestContent);
		client.send(requestMsg);
		Debug.println("client: sends " + requestType + " request to googleServer");
	}

	private IPPort getNameServer() throws UnknownHostException, IOException {
		// TODO Auto-generated method stub
		BufferedReader reader = new BufferedReader(new InputStreamReader(
				new FileInputStream("Announcement.txt"), "UTF-8"));
		IPPort pair = new IPPort();
		pair.ip = InetAddress.getByName(reader.readLine());
		pair.ip = InetAddress.getByName("localhost");
		pair.port = Integer.parseInt(reader.readLine());
		reader.close();
		return pair;
	}

	@Override
	public Message generateMsg(String type, Object content) {
		Message m = new Message();
		m.ip = ip;
		m.port = port;
		m.type = type;
		m.content = content;
		return m;
	}

	@Override
	public void send(Message msg) throws IOException {
		writer.writeObject(msg);
		writer.flush();		
	}

	@Override
	public Message receive() throws IOException {
		try {
			return (Message) reader.readObject();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return null;
		}
	}

	@Override
	public Object process(Message msg) {
		// TODO Auto-generated method stub
		return null;
	}
}