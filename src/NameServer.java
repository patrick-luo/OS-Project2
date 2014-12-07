
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;


public class NameServer extends Server {
	/** the address of the helper
	 * Key: helper address, value: helper type: ready or not ready
	 * */
	private HashMap<IPPort, Boolean> helperList;
	/** the address of the google server*/
	private IPPort googleServerAddress;
	
	public NameServer() throws IOException {
		// initialize all fields
		super(10);
		helperList = new HashMap<IPPort, Boolean>();
		googleServerAddress = null;
		
		//post its address
		Announce();
		//continue receiving and dispatching
		runServer();
	}
	
	public void Announce() throws IOException {
		BufferedWriter w = new BufferedWriter
				(new FileWriter("Announcement.txt"));
		w.write(ip.getHostAddress());
		w.write("\r\n");
		w.write(Integer.toString(port));
		w.close();
		Debug.println("Announcement.txt has been created.");
	}

	@Override
	public void runServer() {
		Debug.println("Nameserver secretary starts running");
		try {
			while (true) {
				try { // accept a new connection from someone
					threadPool.submit(new NameWorker(serverSocket.accept()));
				}
				catch (Exception e) {
					System.err.println("Problems with NameServer connection...terminating");
					System.exit(0);
				}
			}
		}
		finally {
			System.err.println("NameServer shutting down.");
			System.exit(0);
		}
	}
	
	private class NameWorker extends AbstractDude {

		public NameWorker(Socket clientSocket) throws IOException {
			super(clientSocket);
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
				Message replyMsg = generateMsg("reply", "register success");
				try {
					this.send(replyMsg);
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				
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
			
			if (msg.content.equals("google")) { // query the ip & port of google server
				if(googleServerAddress == null) 
					result = generateMsg(type, "no google server available yet");
				else
					result = generateMsg(type, googleServerAddress);
			}
			else if (msg.content.equals("helper")) {
				ArrayList<IPPort> helpers = new ArrayList<IPPort>();
				for (IPPort key : helperList.keySet())
					helpers.add(key);
				result = generateMsg(type, helpers);
				Debug.println("Returning " + helpers.size() + " helpers.");
			}
			send(result);
		}

		private void register(Message msg) {
			// TODO Auto-generated method stub
			IPPort pair = new IPPort();
			pair.ip = msg.ip;
			pair.port = msg.port;
			if(msg.toString().equals("helper"))
				helperList.put(pair, true);
			else if (msg.toString().equals("google"))
				googleServerAddress = pair;
			else {
				System.err.println("Please specify if you are helper or google to register!");
				return;
			}
			Debug.println("NameServer: Register success! from " + pair.toString());
		}
	}
	
	public static void main(String[] args) throws IOException {
		NameServer nameServer = new NameServer();
	}

	
}
