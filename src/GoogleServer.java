import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;

public class GoogleServer extends Server {
	private IPPort nameServer;
	/** used for receiving request from only from clients*/
	private ServerSocket serverSocketSecret;
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

	private class GoogleWorker extends AbstractDude {

		public GoogleWorker(Socket clientSocket) throws IOException {
			super(clientSocket);
		}
		
		public void run(){
			try {
				Message receivedMsg = receive();
				process(receivedMsg);
				//Message replyMsg = generateMsg("reply", "register success");
				//this.send(replyMsg);
				closeSocket();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
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

	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub
		GoogleServer google = new GoogleServer();
	}

}
