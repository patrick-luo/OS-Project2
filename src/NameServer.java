
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.net.Socket;
import java.util.HashMap;


public class NameServer extends Server {
	private HashMap<IPPort, String> helperList;
	private IPPort google;
	
	public NameServer() throws IOException {
		// initialize all fields
		super(10);
		helperList = new HashMap<IPPort, String>();
		google = new IPPort();
		
		// then do something
		Announce();
		runServer();
	}
	
	public void Announce() throws IOException {
		BufferedWriter w = new BufferedWriter
				(new FileWriter("Announcement.txt"));
		w.write(ip.getHostAddress());
		w.write("\r\n");
		w.write(Integer.toString(port));
		w.close();
		System.out.println("Announcement.txt has been created.");
	}

	@Override
	public void runServer() {
		// TODO Auto-generated method stub
		try {
			while (true) {
				try { // accept a new connection from someone
					threadPool.submit(new Dude(serverSocket.accept()));
				}
				catch (Exception e) {
					System.out.println("Problems with NameServer connection...terminating");
					System.exit(0);
				}
			}
		}
		finally {
			System.out.println("NameServer shutting down.");
			System.exit(0);
		}
	}
	
	private class Dude extends AbstractDude {

		public Dude(Socket s) throws IOException {
			super(s);
		}
		
		public void run() {
			try {
				Message receivedMsg = receive();
				process(receivedMsg);
				closeSocket();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
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
		public Object process(Message msg) {
			// TODO Auto-generated method stub
			if(msg.type.equals("register")) {
				register(msg);
			}
			else if (msg.type.equals("query")) {
				try {
					query(msg);
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			else
				System.err.println("Please specify if you are to register or query!");
			
			return null;
		}

		private void query(Message msg) throws IOException {
			String type = "query_result";
			Message result = null;
			
			if (msg.equals("google")) { // query the ip & port of google server
				result = generateMsg(type, google);
			}
			else if (msg.equals("helper")) {
				
			}
			send(result);
		}

		private void register(Message msg) {
			// TODO Auto-generated method stub
			IPPort pair = new IPPort();
			pair.ip = msg.ip;
			pair.port = msg.port;
			if(msg.content.equals("helper"))
				helperList.put(pair, "ready");
			else if (msg.content.equals("google"))
				google = pair;
			else {
				System.err.println("Please specify if you are helper or google to register!");
				return;
			}
			System.out.print("Register success!");
		}
	}
	
	public static void main(String[] args) throws IOException {
		NameServer nameServer = new NameServer();
	}

	
}
