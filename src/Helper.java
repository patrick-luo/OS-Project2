import java.io.IOException;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

public class Helper extends Server {
	private IPPort nameServer;
	private static final int THREAD_POOL_SIZE = 10;
	private HashMap<Integer, ArrayList<Message>> middleResult; // key is xid, value is list of middle results
	
	public Helper() throws IOException {
		super(THREAD_POOL_SIZE);
		middleResult = new HashMap<Integer, ArrayList<Message>>();
		String serverType = "helper";
		nameServer = register(serverType);
		runServer();
	}

	@Override
	public void runServer() {
		Debug.println("Helper secretary starts running");
		try {
			while (true) {
				try { // accept a new connection from someone
					threadPool.submit(new HelperWorker(serverSocket.accept()));
				}
				catch (Exception e) {
					System.err.println("Problems with helper server connection...terminating");
					System.exit(0);
				}
			}
		}
		finally {
			System.err.println("helper server shutting down.");
			System.exit(0);
		}
	}
	
	private class HelperWorker extends AbstractDude {

		public HelperWorker(Socket s) throws IOException {
			super(s);
		}
		
		public void run() {
			try {
				Message task = receive();
				closeSocket();
				process(task);
			} catch (IOException e) {
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
		public Object process(Message task) {
			try {
				if (task.type.equals("indexing")) {
					doIndexMap(task); // begin indexing mapping task
				}
				else if (task.type.equals("searching")) {
					doSearch(task); // begin searching mapping task
				}
				else if (task.type.startsWith("indexing_reducing")) {
					doIndexReduce(task); // begin indexing reducing task
				}
				else {
					// error message
				}
			}
			catch (IOException e) {
				e.printStackTrace();
			}
			
			return null;
		}
		
		/**
		 * 1. put received middle result in middleResult
		 * 2. check if the list is full
		 * 3. if full do reducing
		 * 4. send ask to siqi
		 * @throws IOException 
		 */
		private void doIndexReduce(Message task) throws IOException {
			ArrayList<Message> reduceList = null;
			synchronized (middleResult) {
				ArrayList<Message> middleList = middleResult.get(task.xid);
				middleList.add(task);
				int totalNumReduces = Integer.parseInt(task.type.split(":")[1]);
				if (totalNumReduces == middleList.size()) 
					reduceList = middleResult.remove(task.xid);
			}
			if (reduceList != null) {
				// do reducing here
				List<ConcurrentHashMap<String, String>> reduceTasks = 
						new ArrayList<ConcurrentHashMap<String,String>>();
				for (Message m : reduceList) {
					reduceTasks.add((ConcurrentHashMap<String, String>)m.content);
				}
				GoogleFileManager.reduceIndexing(reduceTasks);
			}
			// send ack to google here
			initIO(new Socket(task.ip, task.port));
			Message indexReply = generateMsg("index_reply", "success");
			send(indexReply);
			closeSocket();
		}

		/**
		 * 1. add xid to middleResult
		 * 2. generate middle result
		 * 3. ask NS for other helpers
		 * 4. send middle result, xid, siqi, and numberOfHelpers to other helpers
		 * @throws IOException 
		 */
		private void doIndexMap(Message task) throws IOException {
			synchronized (middleResult) {
				middleResult.put(task.xid, new ArrayList<Message>());
			}
			ConcurrentHashMap<String, String> indexingMiddleResult = 
					GoogleFileManager.mapIndexing((String[])task.content);
			ArrayList<IPPort> helperList = getHelperList();
			List<ConcurrentHashMap<String, String>> copiesToSend = 
					GoogleFileManager.indexingSplit(indexingMiddleResult, helperList.size());
			sendMiddleResult(task, helperList, copiesToSend);
		}

		private void sendMiddleResult(
				Message task, ArrayList<IPPort> helperList,
				List<ConcurrentHashMap<String,String>> copiesToSend) throws IOException {
			for (int i = 0; i < helperList.size(); i ++) {
				IPPort helper = helperList.get(i);
				ConcurrentHashMap<String,String> copy = copiesToSend.get(i);
				initIO(new Socket(helper.ip, helper.port));
				Message MsgToEachHelper = generateMsg("indexing_reducing:" + helperList.size(), copy);
				MsgToEachHelper.xid = task.xid;
				MsgToEachHelper.ip = task.ip;
				MsgToEachHelper.port = task.port;
				send(MsgToEachHelper);
				closeSocket();
			}
		}

		private ArrayList<IPPort> getHelperList() throws IOException {
			initIO(new Socket(nameServer.ip, nameServer.port));
			Message queryToNS = generateMsg("query", "helper");
			send(queryToNS);
			Message replyFromNS = receive();
			closeSocket();
			Debug.println("Get helpers from NS.");
			return (ArrayList<IPPort>) replyFromNS.content;
		}

		/**
		 * 1. search the inverted indexes of given words
		 * 2. return the search results to google's siqi
		 * @throws IOException 
		 */
		private void doSearch(Message task) throws IOException {
			ConcurrentHashMap<String, List<String>> result = GoogleFileManager.mapSearching((String[])task.content);
			initIO(new Socket(task.ip, task.port));
			Message searchReply = generateMsg("search_result", result);
			send(searchReply);
			closeSocket();
		}
		
	}

	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub
		Helper helper = new Helper();
	}

}
