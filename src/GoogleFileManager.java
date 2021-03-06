import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class GoogleFileManager {

	private String filePath;
	private long begin;
	private long end;
	private RandomAccessFile file;
	FileChannel fc;

	public GoogleFileManager(String filePath, boolean overwrite)
			throws IOException {

		this.filePath = filePath;

		try {
			file = new RandomAccessFile(filePath, "rw");
			if (overwrite) {
				FileChannel fc = file.getChannel();
				FileLock lock = fc.lock();
				this.deleteFile();
				file = new RandomAccessFile(filePath, "rw");
				lock.release();
				fc.close();
			}
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		this.begin = 0;
		this.end = file.length();
		this.fc = file.getChannel();
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
		this.fc = file.getChannel();
	}

	/**
	 * called by the mapper to read the content of the index file to make one
	 * level inverted index
	 * 
	 * @param filePath
	 *            : the name of the index file
	 * @return
	 * @throws IOException
	 */
	public static ConcurrentHashMap<String, String> mapIndexing(String[] task,
			boolean wholeBook) throws IOException {
		ConcurrentHashMap<String, String> oneLevelIndex = new ConcurrentHashMap<String, String>();

		String fileName = task[0];
		GoogleFileManager gm = null;
		if (wholeBook) {
			gm = new GoogleFileManager(fileName, false);
		} else {
			long begin = Long.parseLong(task[1]);
			long end = Long.parseLong(task[2]);
			gm = new GoogleFileManager(fileName, begin, end);
		}
		for (String line; (line = gm.readLine()) != null;) {
			String regexp = "[\\s,;\\n\\t.:?\\d\\(\\)\\[\\]\"\\*\'/]+";
			String[] tokens = line.split(regexp);
			for (int i = 0; i < tokens.length; i++) {
				String token = tokens[i].toLowerCase();
				if (token.length() < 2)
					continue;
				// String fileKey = token.substring(0, 2);
				// if (!invertedIndex.containsKey(fileKey)) {
				// invertedIndex.put(fileKey,
				// new ConcurrentHashMap<String, String>());
				// }
				String pair;
				if (!oneLevelIndex.containsKey(token)) {
					pair = fileName + ":" + 0;
					oneLevelIndex.put(token, pair);
				}
				pair = oneLevelIndex.get(token);
				int cnt = Integer.parseInt(pair.split(":")[1]);
				pair = fileName + ":" + (cnt + 1);
				oneLevelIndex.put(token, pair);
			}
		}

		gm.close();
		return oneLevelIndex;
	}

	public static List<String[]> splitFile(String filePath, int mapperNum) {
		List<String[]> tasks = new ArrayList<String[]>();
		try {
			GoogleFileManager gm = new GoogleFileManager(filePath, false);
			long length = gm.length();
			long interval = length / mapperNum;
			long begin = 0;
			for (int i = 0; i < mapperNum; i++) {
				String[] task;
				if (i < mapperNum - 1)
					task = new String[] { filePath, Long.toString(begin),
							Long.toString(begin + interval) };
				else
					task = new String[] { filePath, Long.toString(begin),
							Long.toString(length) };
				tasks.add(task);
				begin += interval;
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return tasks;
	}

	public static ConcurrentHashMap<String, List<String>> mapSearching(
			String[] task) throws IOException {
		ConcurrentHashMap<String, List<String>> invertedIndex = new ConcurrentHashMap<String, List<String>>();
		for (String word : task) {

			if (word.length() < 2) {
				Debug.println("word too short");
				continue;
			}
			word = word.toLowerCase();
			ConcurrentHashMap<String, List<String>> bigIndex = reducerReadInvertedIndexFromFile(word
					.substring(0, 2));
			if (bigIndex.containsKey(word)) {
				invertedIndex.put(word,
						new ArrayList<String>(bigIndex.get(word)));
			}
		}

		return invertedIndex;
	}

	/**
	 * reducer received a list of partial two level inverted indices from
	 * different mappers and wants to update the corresponding index file
	 * 
	 * @param invertedIndices
	 * @throws IOException
	 */
	public static void reduceIndexing(
			List<ConcurrentHashMap<String, String>> invertedIndicesLevelOne)
			throws IOException {

		ConcurrentHashMap<String, ConcurrentHashMap<String, String>> invertedIndicesLevelTwo = merge(invertedIndicesLevelOne);
		// for(String filePath: invertedIndicesLevelTwo.keySet())
		// Debug.println(filePath);
		// each
		for (String filePath : invertedIndicesLevelTwo.keySet()) {

			// read all stuffs from a file into a one level hash table
			ConcurrentHashMap<String, List<String>> diskIndex = reducerReadInvertedIndexFromFile(filePath);
			// if file doesn't exist, diskIndex will be an empty hash table
			ConcurrentHashMap<String, String> memIndex = invertedIndicesLevelTwo
					.get(filePath);
			// merge the one level hash table with the partial index
			reducerMergeIndices(diskIndex, memIndex);
			// write the merged result(one level hash table) into the disk
			// file
			reducerWriteInvertedIndexToFile(filePath, diskIndex);
		}
	}

	private static ConcurrentHashMap<String, ConcurrentHashMap<String, String>> merge(
			List<ConcurrentHashMap<String, String>> invertedIndicesLevelOne) {
		ConcurrentHashMap<String, ConcurrentHashMap<String, String>> twoLevelMap = new ConcurrentHashMap<String, ConcurrentHashMap<String, String>>();
		for (ConcurrentHashMap<String, String> oneLevelMap : invertedIndicesLevelOne) {
			for (String word : oneLevelMap.keySet()) {
				if (word.equals("###"))
					break;
				String filekey = word.substring(0, 2);
				if (!twoLevelMap.containsKey(filekey)) {
					twoLevelMap.put(filekey,
							new ConcurrentHashMap<String, String>());
					twoLevelMap.get(filekey).put(word,
							new String(oneLevelMap.get(word)));
				} else {
					if (!twoLevelMap.get(filekey).containsKey(word)) {
						twoLevelMap.get(filekey).put(word,
								new String(oneLevelMap.get(word)));
					} else {
						int cnt = Integer.parseInt(twoLevelMap.get(filekey)
								.get(word).split(":")[1]);
						String[] oldPair = oneLevelMap.get(word).split(":");
						cnt += Integer.parseInt(oldPair[1]);
						String updatedPair = oldPair[0] + ":" + cnt;
						twoLevelMap.get(filekey).put(word, updatedPair);
					}
				}
			}
		}
		return twoLevelMap;
	}

	public static ConcurrentHashMap<String, List<String>> reduceSearching(
			List<ConcurrentHashMap<String, List<String>>> invertedIndices)
			throws IOException {
		ConcurrentHashMap<String, List<String>> result = new ConcurrentHashMap<String, List<String>>();
		for (ConcurrentHashMap<String, List<String>> invertedIndex : invertedIndices) {
			for (String key : invertedIndex.keySet()) {
				result.put(key, invertedIndex.get(key));
			}
		}

		return result;
	}

	public static String[] pageRank(
			ConcurrentHashMap<String, List<String>> invertedIndex, String[] keys) {
		List<String> rank = new ArrayList<String>();
		HashMap<String, Integer> documents = new HashMap<String, Integer>();
		for (String word : invertedIndex.keySet()) {
			List<String> list = invertedIndex.get(word);
			for (String str : list) {
				String doc = str.split(":")[0];
				int cnt = Integer.parseInt(str.split(":")[1]);
				if (documents.containsKey(doc))
					documents.put(doc, documents.get(doc) + cnt);
				else
					documents.put(doc, cnt);
			}
		}
		return sortDocuments(documents);
	}

	private static String[] sortDocuments(HashMap<String, Integer> documents) {
		// TODO Auto-generated method stub
		Set<String> keyset = documents.keySet();
		String[] list = new String[keyset.size()];
		int i = 0;
		for(String key: keyset) list[i++] = key;
		Quick.setStandard(documents);
		Quick.sort(list);
		printRankResult(list, documents);
		return list;
	}

	private static void printRankResult(String[] list,
			HashMap<String, Integer> documents) {
		for (String doc : list) {
			Debug.println(doc + "[" + documents.get(doc) + "]");
		}
	}

	public static List<ConcurrentHashMap<String, String>> indexingSplit(
			ConcurrentHashMap<String, String> bigMap, int partNum) {
		ArrayList<ConcurrentHashMap<String, String>> smallMapList = new ArrayList<ConcurrentHashMap<String, String>>();
		for (int i = 0; i < partNum; i++) {
			smallMapList.add(new ConcurrentHashMap<String, String>());
		}
		for (String word : bigMap.keySet()) {
			int index = (word.charAt(0) - 'a') % partNum;
			smallMapList.get(index).put(word, new String(bigMap.get(word)));
		}
		for (int i = 0; i < partNum; i++) {
			if (smallMapList.get(i).isEmpty())
				smallMapList.get(i).put("###", "qiaojie:-1");
		}
		return smallMapList;
	}

	private void close() throws IOException {
		fc.close();
		file.close();
	}

	private long length() throws IOException {
		return file.length();
	}

	public static void main(String[] args) throws IOException {

		// GoogleFileManager gms = new GoogleFileManager("haha.txt", false);
		// long length = gms.length();
		// GoogleFileManager gms1, gms2, gms3;
		// gms1 = new GoogleFileManager("haha.txt", 0, length / 3);
		// gms2 = new GoogleFileManager("haha.txt", length / 3, length * 2 / 3);
		// gms3 = new GoogleFileManager("haha.txt", length * 2 / 3, length);
		// for (String line; (line = gms1.readLine()) != null;)
		// System.out.println(line);
		// System.out.println("-----1---------");
		// gms1.close();
		//
		// for (String line; (line = gms2.readLine()) != null;)
		// System.out.println(line);
		// System.out.println("-----2---------");
		// gms2.close();
		//
		// for (String line; (line = gms3.readLine()) != null;)
		// System.out.println(line);
		// System.out.println("-----3---------");
		// gms3.close();
		//
		// gms1 = new GoogleFileManager("haha.txt", false);
		// for (int i = 10001; i < 20000; i++) {
		// gms1.writeLine(Integer.toString(i));
		// }
		// gms1.close();
		//
		// gms1 = new GoogleFileManager("haha.txt", 0, length / 3);
		// gms2 = new GoogleFileManager("haha.txt", length / 3, length * 2 / 3);
		// gms3 = new GoogleFileManager("haha.txt", length * 2 / 3, length);
		// for (String line; (line = gms1.readLine()) != null;)
		// System.out.println(line);
		// System.out.println("-----1---------");
		// gms1.close();
		//
		// for (String line; (line = gms2.readLine()) != null;)
		// System.out.println(line);
		// System.out.println("-----2---------");
		// gms2.close();
		//
		// for (String line; (line = gms3.readLine()) != null;)
		// System.out.println(line);
		// System.out.println("-----3---------");
		// gms3.close();
		ConcurrentHashMap<String, String> book = mapIndexing(
				new String[] { "bible.txt" }, true);
		List<ConcurrentHashMap<String, String>> booklist = new ArrayList<ConcurrentHashMap<String, String>>();
		booklist.add(book);
		// reduceIndexing(booklist);
		ConcurrentHashMap<String, List<String>> interResult1 = mapSearching("what the hell"
				.split(" "));
		ConcurrentHashMap<String, List<String>> interResult2 = mapSearching("how are you is that"
				.split(" "));
		List<ConcurrentHashMap<String, List<String>>> interResults = new ArrayList<ConcurrentHashMap<String, List<String>>>();
		interResults.add(interResult1);
		//interResults.add(interResult2);
		ConcurrentHashMap<String, List<String>> result = reduceSearching(interResults);
		for (String key : result.keySet()) {
			Debug.println(key + "=" + result.get(key));
		}
		String[] rankedPages = pageRank(result, "what the hell".split(" "));
	}

	/**
	 * read one line from the file at a time until it reaches the end may read
	 * more words beyond the end
	 * 
	 * @return
	 * @throws IOException
	 */
	private String readLine() throws IOException {
		String line = null;

		FileLock lock = fc.lock();
		long pos = file.getFilePointer();
		if (pos >= end)
			return null;
		line = file.readLine();
		lock.release();
		return line;
	}

	private void deleteFile() throws IOException {
		File file = new File(filePath);
		file.delete();
	}

	private void writeLine(String str) throws IOException {
		FileLock lock = fc.lock();
		file.writeBytes(str + "\n");
		lock.release();
	}

	/**
	 * called by the reducer to overwrite the index file
	 * 
	 * @param twoLevelIndex
	 * @throws IOException
	 */
	private static void reducerWriteInvertedIndexToFile(String filePath,
			ConcurrentHashMap<String, List<String>> oneLevelIndex)
			throws IOException {
		GoogleFileManager gm = new GoogleFileManager(filePath, true);
		for (String word : oneLevelIndex.keySet()) {
			StringBuilder sb = new StringBuilder();
			sb.append(word);
			List<String> list = oneLevelIndex.get(word);
			for (String pair : list) {
				sb.append("\t" + pair);
			}
			gm.writeLine(sb.toString());
		}
		gm.close();
	}

	/**
	 * called by the reducer to read the content of the index file into memory
	 * 
	 * @param filePath
	 * @return
	 * @throws IOException
	 */
	private static ConcurrentHashMap<String, List<String>> reducerReadInvertedIndexFromFile(
			String filePath) throws IOException {
		GoogleFileManager gm = new GoogleFileManager(filePath, false);
		ConcurrentHashMap<String, List<String>> invertedIndex = new ConcurrentHashMap<String, List<String>>();
		for (String line; (line = gm.readLine()) != null;) {
			String[] tokens = line.split("\t");
			List<String> list = new ArrayList<String>();
			for (int i = 1; i < tokens.length; i++) {
				list.add(tokens[i].toLowerCase());
			}
			invertedIndex.put(tokens[0].toLowerCase(), list);
		}
		gm.close();
		return invertedIndex;
	}

	/**
	 * assume no duplicated book will be indexed so all the value of the
	 * diskIndex will be list with unique value,
	 * 
	 * @param diskIndex
	 * @param memIndex
	 */
	private static void reducerMergeIndices(
			ConcurrentHashMap<String, List<String>> diskIndex,
			ConcurrentHashMap<String, String> memIndex) {

		for (String word : memIndex.keySet()) {
			String memPair = memIndex.get(word);
			if (!diskIndex.containsKey(word)) {
				List<String> pairList = new ArrayList<String>();
				pairList.add(memPair);
				diskIndex.put(word, pairList);
			} else {
				List<String> diskList = diskIndex.get(word);
				diskList.add(memPair);
			}
		}
	}

}
