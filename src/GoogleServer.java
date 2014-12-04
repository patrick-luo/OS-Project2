import java.io.IOException;
import java.net.Socket;
import java.util.ArrayList;


public class GoogleServer extends Server {
	private IPPort nameServer;
	private ArrayList<Message> queue; // message queue
	
	public GoogleServer() throws IOException {
		super(10);
		nameServer = register("google");
		queue = new ArrayList<Message>();
		runServer();
	}
	
	@Override
	public void runServer() {
		try {
			threadPool.submit(new Secretary());
			while (true) {
				Message request = null;
				synchronized (queue) {
					if (queue.size() != 0) {
						request = queue.remove(0);
					}
				}
				if (request != null)
					process(request);
			}
		}
		finally {
			System.out.println("GoogleServer shutting down.");
			System.exit(0);
		}
	}
	private void process(Message request) {
		// TODO Auto-generated method stub
		if (request.type.equals("index"))
			threadPool.submit(new IndexMaster(request));
		else if (request.type.equals("search"))
			threadPool.submit(new SearchMaster(request));
		else
			System.out.println("Please specify index or search");
	}
	
	private class IndexMaster extends Thread {
		private Message request;
		public IndexMaster(Message request) {
			this.request = request;
		}
		public void run() {
			
		}
		
	}
	
	private class SearchMaster extends Thread {
		private Message request;
		public SearchMaster(Message request) {
			this.request = request;
		}
		
	}
	
	/**
	 * This class is responsible for receiving messages only.
	 * She will call a dude to receive each message.
	 * After receiving each message, the dude would store it into
	 * the message queue.
	 */
	private class Secretary extends Thread {
		
		public void run() {
			while (true) {
				try { // accept a new connection from someone
					threadPool.submit(new ReceiveDude(serverSocket.accept()));
				}
				catch (Exception e) {
					System.out.println("Problems with Receiver...terminating");
					System.exit(0);
				}
			}
		}
		
		private class ReceiveDude extends AbstractDude {

			public ReceiveDude(Socket s) throws IOException {
				super(s);
			}
			
			public void run() {
				try {
					process(receive());
					closeSocket();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}

			@Override
			public Message generateMsg(String type, Object content) {return null;}

			@Override
			public Object process(Message msg) {
				synchronized (queue) {
					queue.add(msg);
				}
				System.out.println("New request has been put into queue, waiting to be processed...");
				return null;
			}
		}
	}

	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub
		GoogleServer google = new GoogleServer();
	}

	

}
