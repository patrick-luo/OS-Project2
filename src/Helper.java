import java.io.IOException;

public class Helper extends Server {
	private IPPort nameServer;
	private static final int THREAD_POOL_SIZE = 10;
	
	public Helper() throws IOException {
		super(THREAD_POOL_SIZE);
		String serverType = "helper";
		nameServer = register(serverType);
	//	runServer();
	}

	@Override
	public void runServer() {
//		Debug.println("Google secretary: 'starts running'");
//		try {
//			while (true) {
//				try { // accept a new connection from someone
//					threadPool.submit(new GoogleWorker(serverSocket.accept()));
//				}
//				catch (Exception e) {
//					System.out.println(" threadPool.submit(new GoogleWorker(serverSocket.accept()));");
//					System.exit(0);
//				}
//			}
//		}
//		finally {
//			System.out.println("google server shutting down.");
//			System.exit(0);
//		}
	}

	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub
		Helper helper = new Helper();
	}

}
