import java.io.IOException;
import java.net.ConnectException;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;

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
			taskList = new ArrayList<String[]>();
			helperList = null;
			replyList = new ArrayList<Object>();
		}
		
		public void run() {
			try {
				request = receive();
				Debug.println("Successfully receive a requst from one client, xid = " + xid);
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
				boolean haveHelper = verifyHelperList();
				if (haveHelper) {
					splitTask();
					sendTaskToHelper();
					receiveReplyFromHelper();
					sendReplyToClient(true);
				}
				else
					sendReplyToClient(false);
				closeSocket();
			}
			catch (IOException e) {
				e.printStackTrace();
			}
			return null;
		}

		private boolean verifyHelperList() throws IOException {
			Debug.println("Start verifying helpers...");
			for (int i = 0; i < helperList.size(); i ++) {
				Debug.println("Verifying helper #" + i + ": ");
				IPPort helper = helperList.get(i);
				Socket tryConnection = null;
				try {
					tryConnection = new Socket(helper.ip, helper.port);
				}
				catch (ConnectException e) {
					Debug.println("Helper #" + i + " is dead, so remove it from NS.");
					helperList.remove(i);
					removeHelperFromNS(helper);
				}
				if (tryConnection != null) {
					chi = new Communicator(tryConnection);
					chi.send(chi.generateMsg("verify", "google"));
					chi.receive();
					Debug.println("Helper #" + i + " is alive.");
					chi.closeSocket();
				}
			}
			if (helperList.size() == 0) {
				Debug.println("No available helper.");
				return false;
			}
				
			else
				return true;
		}

		private void removeHelperFromNS(IPPort helper) throws IOException {
			chi = new Communicator(new Socket(nameServer.ip, nameServer.port));
			chi.send(generateMsg("remove", helper));
			chi.closeSocket();
		}

		private void sendReplyToClient(boolean success) throws IOException {
			if (!success) {
				Debug.println("Send reply to client, xid = " + xid);
				Message reply = generateMsg("reply", "Failure, no available helper.");
				send(reply);
				return;
			}
			if(request.type.equals("indexing")){
				Message reply = generateMsg("indexing", "indexing finished");
				send(reply);
			}
			else if(request.type.equals("searching")){
				List<ConcurrentHashMap<String, List<String>>> smallHashList =
						new ArrayList<ConcurrentHashMap<String,List<String>>>();
				for (Object smallHash : replyList) {
					smallHashList.add((ConcurrentHashMap<String, List<String>>)smallHash);
				}
				String[] searchKeys = (String[]) request.content;
				String[] rankedResult = GoogleFileManager.pageRank(
						GoogleFileManager.reduceSearching(smallHashList), searchKeys);
				Message reply = generateMsg("searching", rankedResult);
				send(reply);
			}
			else {
				System.err.println("Please specify if you are searching or indexing");
			}
			Debug.println("Send reply to client, xid = " + xid);
		}

		private void receiveReplyFromHelper() throws IOException {
			for(int i = 0; i < helperList.size(); i ++) {
				chi = new Communicator(siqi.accept());
				Message reply = chi.receive();
				replyList.add(reply.content);
				chi.closeSocket();
				Debug.println("Done receiving result from helper #" + i + ", xid = " + xid);
			}
		}

		private void splitTask() {
			String[] content = (String[]) request.content;
			if (request.type.equals("indexing")) {
				String filePath = content[0];
				taskList = (ArrayList<String[]>) GoogleFileManager.splitFile(filePath, helperList.size());
			}
			else if (request.type.equals("searching")) {
				// Assumption: keywords are split by ';'
				String[] wordList = content;
			//	System.out.println(content);
				
				if (helperList.size() == 1)
					taskList.add(wordList);
				else {
					int numPerHelper;
					if (wordList.length % helperList.size() == 0) {
						numPerHelper = wordList.length / helperList.size();
				//		System.out.println("Here 1");
					}
					else {
						int integer = wordList.length / helperList.size();
						double decimal = wordList.length * 1.0 / helperList.size() - integer;
						numPerHelper = decimal < 0.5 ? integer : integer + 1;
					}
					for (int i = 0, cnt = 0; i < helperList.size(); i ++) {
				//		System.out.println("Here 2");
						String[] task;
						if (i != helperList.size() - 1) 
							task = new String[numPerHelper];
						else
							task = new String[wordList.length - numPerHelper * (helperList.size() - 1)];
		
						for(int j = 0; j < task.length; j ++) {
							task[j] = wordList[cnt ++];
						}
					//	System.out.println("Here 3");
					//	System.out.println(task.length);
					//	System.out.println(task[0]);
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
