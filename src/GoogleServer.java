import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;

public class GoogleServer extends Server {
	private IPPort nameServer;
	/** used for receiving request from only from clients*/
	private static final int THREAD_POOL_SIZE = 10;

	public GoogleServer() throws IOException {
		super(THREAD_POOL_SIZE);
		String serverType = "google";
		this.nameServer = register(serverType);
		runServer();
	}

	/**
	 * google server's main thread is a secretary
	 */
	public void runServer() {
		Debug.println("Google secretary: 'starts running'");
		try {
			while (true) {
				try { // accept a new connection from someone
					threadPool.submit(new GoogleWorker(serverSocket.accept()));
				}
				catch (Exception e) {
					System.out.println(" threadPool.submit(new GoogleWorker(serverSocket.accept()));");
					System.exit(0);
				}
			}
		}
		finally {
			System.out.println("google server shutting down.");
			System.exit(0);
		}
	}

	private class GoogleWorker extends AbstractDude {
		
		private Communicator chi;
		
		private class Communicator extends AbstractDude {

			public Communicator(Socket s) throws IOException {
				super(s);
				chi = new Communicator(null);
			}

			@Override
			public Message generateMsg(String type, Object content) {
				// TODO Auto-generated method stub
				return null;
			}

			@Override
			public Object process(Message msg) {
				// TODO Auto-generated method stub
				return null;
			}
			
		}

		public GoogleWorker(Socket clientSocket) throws IOException {
			super(clientSocket);
		}
		
		public void run(){
			try {
				Message request = receive();
				process(request);
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

		/**
		 * process a request from one client. either "indexing" or "searching" 
		 */
		@Override
		public Object process(Message request) {
			ArrayList<IPPort> helperList = getHelperList();
			ServerSocket siqi = new ServerSocket(0);
			sendTaskToHelper(helperList);
			receiveReplyFromHelper();
			
			
			
			
			
			
			if(request.type.equals("indexing")){
				//TODO assume indexing finished immediately
				Debug.println("google server: receive indexing request");
				Message reply = generateMsg("reply", "indexing finished");
				try {
					send(reply);
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			else if(request.type.equals("searching")){
				Debug.println("google server: receive searching request");
				Message reply = generateMsg("reply", "searching finished");
				try {
					send(reply);
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			else {
				System.err.println("Please specify if you are searching or indexing");
				return null;
			}
			return null;
		}
		

	}

	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub
		GoogleServer google = new GoogleServer();
	}

}
