import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Random;

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
		Debug.println("Google secretary starts running");
		try {
			Random rand = new Random();
			while (true) {
				try { // accept a new connection from someone
					threadPool.submit(new GoogleWorker(serverSocket.accept(), rand.nextInt()));
				}
				catch (Exception e) {
					System.err.println("Problem with google server connection...terminating.");
					System.exit(0);
				}
			}
		}
		finally {
			System.err.println("google server shutting down.");
			System.exit(0);
		}
	}

	private class GoogleWorker extends AbstractDude {
		
		Message request;
		int xid;
		private Communicator chi;
		private ServerSocket siqi;
		ArrayList<String[]> taskList;
		ArrayList<IPPort> helperList;
		ArrayList<Object> replyList;
		
		private class Communicator extends AbstractDude {

			public Communicator(Socket s) throws IOException {
				super(s);
			}

			@Override
			public Message generateMsg(String type, Object content) {
				Message m = new Message();
				m.xid = xid;
				m.ip = ip;
				m.port = port;
				m.type = type;
				m.content = content;
				return m;
			}

			@Override
			public Object process(Message msg) {return null;}
			
		}

		public GoogleWorker(Socket clientSocket, int xid) throws IOException {
			super(clientSocket);
			request = null;
			this.xid = xid;
			chi = null;
			siqi = new ServerSocket(0);
			taskList = null;
			helperList = null;
			replyList = new ArrayList<Object>();
		}
		
		public void run() {
			try {
				request = receive();
				Debug.println("Successfully receive a requst from one client");
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
			try {
				getHelperList();
				splitTask();
				sendTaskToHelper();
				receiveReplyFromHelper();
				sendReplyToClient();
				closeSocket();
			}
			catch (IOException e) {
				e.printStackTrace();
			}
			return null;
		}

		private void sendReplyToClient() throws IOException {
			if(request.type.equals("indexing")){
				Message reply = generateMsg("reply", "indexing finished");
				send(reply);
			}
			else if(request.type.equals("searching")){
				Message reply = generateMsg("reply", "searching finished");
				send(reply);
			}
			else {
				System.err.println("Please specify if you are searching or indexing");
			}
			Debug.println("Send reply to client, xid = " + xid);
		}

		private void receiveReplyFromHelper() throws IOException {
		//	System.out.println("siqi port: " + siqi.getLocalPort());
			for(int i = 0; i < helperList.size(); i ++) {
				chi = new Communicator(siqi.accept());
			//	System.out.println("Here 2");
				Message reply = chi.receive();
			//	System.out.println("Here 3");
				replyList.add(reply.content);
			//	System.out.println("Here 4");
				chi.closeSocket();
				Debug.println("Done receiving result from helper #" + i + ", xid = " + xid);
			}
		//	System.out.println("Here");
		}

		private void splitTask() {
			String content = (String) request.content;
			if (request.type.equals("indexing")) {
				String filePath = content;
				taskList = (ArrayList<String[]>) GoogleFileManager.splitFile(filePath, helperList.size());
			}
			else if (request.type.equals("searching")) {
				// Assumption: keywords are split by ';'
				String[] wordList = content.split(";");
				
				if (helperList.size() == 1)
					taskList.add(wordList);
				else {
					int numPerHelper;
					if (wordList.length % helperList.size() == 0) {
						numPerHelper = wordList.length / helperList.size();
					}
					else {
						int integer = wordList.length / helperList.size();
						double decimal = wordList.length * 1.0 / helperList.size() - integer;
						numPerHelper = decimal < 0.5 ? integer : integer + 1;
					}
					for (int i = 0, cnt = 0; i < helperList.size(); i ++) {
						String[] task;
						if (i != helperList.size() - 2) 
							task = new String[numPerHelper];
						else
							task = new String[wordList.length - numPerHelper * (helperList.size() - 1)];
		
						for(int j = 0; j < task.length; j ++) {
							task[j] = wordList[cnt ++];
						}
						taskList.add(task);
					}
				}
				
			}
			else {
				// error information
			}
			Debug.println("Have split tasks for xid = " + xid);
		}

		private void sendTaskToHelper() throws IOException {
			for (int i = 0; i < helperList.size(); i ++) {
				/**
				 * for each task message, its ip & port indicate siqi's ip & port;
				 * xid is transcation ID;
				 * type is "indexing" or "searching";
				 * content is a string array.
				 */
				chi = new Communicator(new Socket(helperList.get(i).ip, helperList.get(i).port));
				Message task = chi.generateMsg(request.type, taskList.get(i));
				task.port = siqi.getLocalPort(); // redirect port of receiving result to be siqi's port
				chi.send(task);
				chi.closeSocket();
				Debug.println("Sent task to helper #" + i + ", xid = " + xid);
			}
		}

		private void getHelperList() throws IOException {
			chi = new Communicator(new Socket(nameServer.ip, nameServer.port));
			Message queryToNS = chi.generateMsg("query", "helper");
			chi.send(queryToNS);
			Message replyFromNS = chi.receive();
			chi.closeSocket();
			helperList = (ArrayList<IPPort>) replyFromNS.content;
			Debug.println("Get " + helperList.size() + " helpers from NS.");
		}
		

	}

	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub
		GoogleServer google = new GoogleServer();
	}

}
