import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectOutputStream;
import java.io.UnsupportedEncodingException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;


public abstract class Server {
	protected InetAddress ip;
	protected int port;
	protected int poolSize;
	protected ExecutorService threadPool;
	protected ServerSocket serverSocket;
	protected String serverType; // either "helper" or "google"

	
	public Server(int poolSize) throws IOException {
		// initialize all fields
		ip = InetAddress.getLocalHost();
		serverSocket = new ServerSocket(0);
		this.port = serverSocket.getLocalPort();
		Debug.println("Server socket port = " + this.port);
		this.poolSize = poolSize;
		threadPool = Executors.newFixedThreadPool(this.poolSize);
	}
	/**
	 * register itself to NameServer and return the (IP, Port) of NameServer
	 */
	public IPPort register(String content) throws UnknownHostException, IOException {
		IPPort nameServer = getNameServer();
		Socket clientSocket = new Socket(nameServer.ip, nameServer.port);
		RegisterDude dude = new RegisterDude(clientSocket, content);
		threadPool.submit(dude);
		return nameServer;
	}
	
	private IPPort getNameServer() throws UnknownHostException, IOException {
		// TODO Auto-generated method stub
		BufferedReader reader = new BufferedReader
				(new InputStreamReader(new FileInputStream("Announcement.txt"), "UTF-8"));
		IPPort pair = new IPPort();
		pair.ip = InetAddress.getByName(reader.readLine());
		pair.ip = InetAddress.getByName("localhost");
		pair.port = Integer.parseInt(reader.readLine());
		reader.close();
		return pair;
	}

	private class RegisterDude extends AbstractDude {
		
		public RegisterDude(Socket s, String content) throws IOException {
			super(s);
			serverType = content;
		}

		public void run() {
			// TODO Auto-generated method stub
			Message registerInfo = generateMsg("register", serverType);
			try {
				send(registerInfo);
				Message replyMsgFromNS = receive();
				String replyStr = replyMsgFromNS.toString();
				Debug.println(serverType + ": reply from the NS: " + replyStr);
				closeSocket();//closeSocket right after sending the register request
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
//			System.out.println("Hello Dude!");
		}

		@Override
		public Message generateMsg(String type, Object content) {
			// TODO Auto-generated method stub
			Message m = new Message();
			m.ip = ip;
			m.port = port;
			m.type = type;
			m.content = content;
			return m;
		}

		@Override
		public Object process(Message msg) {return null;}
		
	}
	
	public abstract void runServer();
	
}
