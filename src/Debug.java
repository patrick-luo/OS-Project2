
public class Debug {
	public static boolean DEBUG = true;
	public static int infoCnt = 0;
	public static void println(String str){
		System.out.println("INFO " + infoCnt++ + ": " + str);
	}
}