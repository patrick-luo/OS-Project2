import java.io.IOException;


public class Helper extends Server {
	private IPPort nameServer;
	
	public Helper() throws IOException {
		super(9);
		nameServer = register("helper");
		runServer();
	}

	@Override
	public void runServer() {
		// TODO Auto-generated method stub
		
	}

	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub
		Helper helper = new Helper();
	}

}
