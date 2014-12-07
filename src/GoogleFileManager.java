import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;

public class GoogleFileManager {

	private String filePath;
	private long begin;
	private long end;
	private RandomAccessFile file;

	public GoogleFileManager(String filePath) throws IOException {
		this.filePath = filePath;
		try {
			file = new RandomAccessFile(filePath, "rw");
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		this.begin = 0;
		this.end = file.length();
		// file.seek(this.begin);
	}

	public GoogleFileManager(String filePath, long begin, long end)
			throws IOException {
		this.filePath = filePath;
		try {
			file = new RandomAccessFile(filePath, "rw");
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		this.begin = begin;
		this.end = end;
		file.seek(this.begin);
	}

	public void close() throws IOException {
		file.close();
	}

	public long length() throws IOException {
		return file.length();
	}

	public static void main(String[] args) throws IOException {

		GoogleFileManager gms = new GoogleFileManager("haha.txt");
		long length = gms.length();
		GoogleFileManager gms1, gms2, gms3;
		gms1 = new GoogleFileManager("haha.txt", 0, length / 3);
		gms2 = new GoogleFileManager("haha.txt", length / 3, length * 2 / 3);
		gms3 = new GoogleFileManager("haha.txt", length * 2 / 3, length);
		for (String line; (line = gms2.readLine()) != null;)
			System.out.println(line);
		
		deleteFile("haha.txt");
		
		gms1 = new GoogleFileManager("haha.txt");
		for(int i = 0; i < 10000; i++){
			gms1.writeLine(Integer.toString(i));
		}
	}

	/**
	 * read one line from the file at a time until it reaches the end may read
	 * more words beyond the end
	 * 
	 * @return
	 * @throws IOException
	 */
	public String readLine() throws IOException {

		long pos = file.getFilePointer();
		if (pos >= end)
			return null;

		return file.readLine();
	}

	public static void deleteFile(String filePath) {
		File file = new File(filePath);
		file.delete();
	}

	public void writeLine(String str) throws IOException {

		file.writeBytes(str + "\n");

	}
}
