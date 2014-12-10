import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;


public class ThreadTest {

	private class Dude extends Thread {
		public void run() {
			System.out.println("Hello");
		}
	}
	public void runDude() {
		ExecutorService es = Executors.newFixedThreadPool(10);
		Dude d = new Dude();
		es.submit(d);
	}
	public ThreadTest() {
		runDude();
	}
	public static void main(String[] args) throws UnknownHostException {
		// TODO Auto-generated method stub
	//	ThreadTest t = new ThreadTest();
		InetAddress ip1 = InetAddress.getLocalHost();
		InetAddress ip2 = InetAddress.getLocalHost();
		System.out.println(ip1.equals(ip2));
	}

}
