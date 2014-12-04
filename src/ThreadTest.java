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
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		ThreadTest t = new ThreadTest();
	}

}
