import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

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

	/**
	 * called by the mapper to read the content of the index file to make one
	 * level inverted index
	 * 
	 * @param filePath
	 *            : the name of the index file
	 * @return
	 * @throws IOException
	 */
	public static ConcurrentHashMap<String, String> mapIndexing(String[] task)
			throws IOException {
		ConcurrentHashMap<String, String> oneLevelIndex = new ConcurrentHashMap<String, String>();

		String fileName = task[0];
		long begin = Long.parseLong(task[1]);
		long end = Long.parseLong(task[2]);
		GoogleFileManager gm = new GoogleFileManager(fileName, begin, end);
		for (String line; (line = gm.readLine()) != null;) {
			String regexp = "[\\s,;\\n\\t]+";
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
				}
				pair = oneLevelIndex.get(token);
				int cnt = Integer.parseInt(pair.split(":")[1]);
				pair = fileName + ":" + (cnt + 1);
				oneLevelIndex.put(token, pair);
			}
		}

		return oneLevelIndex;
	}

	public static List<String[]> splitFile(String filePath, int mapperNum) {
		return null;
	}

	public static ConcurrentHashMap<String, List<String>> mapSearching(
			String[] task) throws IOException {
		ConcurrentHashMap<String, List<String>> invertedIndex = new ConcurrentHashMap<String, List<String>>();
		for (String word : task) {
			if (word.length() < 2)
				Debug.println("word too short");

			ConcurrentHashMap<String, List<String>> bigIndex = reducerReadInvertedIndexFromFile(word);
			if (bigIndex.contains(word)) {
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
		// each
		for (String fileName : invertedIndicesLevelTwo.keySet()) {

			// read all stuffs from a file into a one level hash table
			String filePath = fileName.charAt(0) + "/" + fileName;
			ConcurrentHashMap<String, List<String>> diskIndex = reducerReadInvertedIndexFromFile(filePath);
			ConcurrentHashMap<String, String> memIndex = invertedIndicesLevelTwo
					.get(fileName);
			// merge the one level hash table with the partial index
			reducerMergeIndices(diskIndex, memIndex);
			// write the merged result(one level hash table) into the disk
			// file
			reducerWriteInvertedIndexToFile(filePath, diskIndex);
		}
	}

	private static ConcurrentHashMap<String, ConcurrentHashMap<String, String>> merge(
			List<ConcurrentHashMap<String, String>> invertedIndicesLevelOne) {
		ConcurrentHashMap<String, ConcurrentHashMap<String, String>> twoLevelMap = 
				new ConcurrentHashMap<String, ConcurrentHashMap<String,String>>();
		for (ConcurrentHashMap<String, String> oneLevelMap : invertedIndicesLevelOne) {
			for (String word : oneLevelMap.keySet()) {
				if (word.equals("###"))
					break;
				String filekey = word.substring(0, 2);
				if (!twoLevelMap.containsKey(filekey)) {
					twoLevelMap.put(filekey, new ConcurrentHashMap<String, String>());
					twoLevelMap.get(filekey).put(word, oneLevelMap.get(word));
				}
				else {
					if (!twoLevelMap.get(filekey).containsKey(word)) {
						twoLevelMap.get(filekey).put(word, oneLevelMap.get(word));
					}
					else {
						int cnt = Integer.parseInt(twoLevelMap.get(filekey).get(word).split(":")[1]);
						String[] oldPair = oneLevelMap.get(word).split(":");
						cnt += Integer.parseInt(oldPair[1]);
						String updatedPair = oldPair[0] + ":" + cnt;
						twoLevelMap.get(filekey).put(word, updatedPair);
					}
				}
			}
		}
		return null;
	}

	public static ConcurrentHashMap<String, List<String>> reduceSearching(
			List<ConcurrentHashMap<String, List<String>>> invertedIndices)
			throws IOException {
		return null;
		// sort
	}

	public static List<ConcurrentHashMap<String, String>> indexingSplit(
			ConcurrentHashMap<String, String> bigMap, int partNum) {
		ArrayList<ConcurrentHashMap<String, String>> smallMapList = new ArrayList<ConcurrentHashMap<String,String>>();
		for (int i = 0; i < partNum; i ++) {
			smallMapList.add(new ConcurrentHashMap<String, String>());
		}
		for (String word : bigMap.keySet()) {
			int index = (word.charAt(0) - 'a') % partNum;
			smallMapList.get(index).put(word, bigMap.get(word));
		}
		for (int i = 0; i < partNum; i ++) {
			if (smallMapList.get(i).isEmpty())
				smallMapList.get(i).put("###", "qiaojie:-1");
		}
		return smallMapList;
	}

	private void close() throws IOException {
		file.close();
	}

	private long length() throws IOException {
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

		// deleteFile("haha.txt");
		/*
		 * gms1 = new GoogleFileManager("haha.txt"); for(int i = 0; i < 10000;
		 * i++){ gms1.writeLine(Integer.toString(i)); }
		 */
	}

	/**
	 * read one line from the file at a time until it reaches the end may read
	 * more words beyond the end
	 * 
	 * @return
	 * @throws IOException
	 */
	private String readLine() throws IOException {

		long pos = file.getFilePointer();
		if (pos >= end)
			return null;

		return file.readLine();
	}

	private static void deleteFile(String filePath) {
		File file = new File(filePath);
		file.delete();
	}

	private void writeLine(String str) throws IOException {
		file.writeBytes(str + "\n");
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
		deleteFile(filePath);
		GoogleFileManager gm = new GoogleFileManager(filePath);
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
		GoogleFileManager gm = new GoogleFileManager(filePath);
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

	private static void reducerMergeIndices(
			ConcurrentHashMap<String, List<String>> diskIndex,
			ConcurrentHashMap<String, String> memIndex) {

		for (String word : memIndex.keySet()) {
			String memPair = memIndex.get(word);
			String memDoc = memPair.split(":")[0];
			int memCnt = Integer.parseInt(memPair.split(":")[1]);

			List<String> diskList = diskIndex.get(word);
			int i;
			for (i = 0; i < diskList.size(); i++) {
				String diskPair = diskList.get(i);
				String diskDoc = diskPair.split(":")[0];
				int diskCnt = Integer.parseInt(diskPair.split(":")[1]);
				if (diskDoc.compareTo(memDoc) == 0) {
					diskList.set(i, memDoc + ":" + (diskCnt + memCnt));
					break;
				} else if (diskDoc.compareTo(memDoc) > 0) {
					diskList.add(i, memPair);
					break;
				}
			}
			if (i == diskList.size())
				diskList.add(i, memPair);
		}

	}
	/*
	 * private static List<String> merge(List<String> memList, List<String>
	 * diskList) { int m = memList.size(), n = diskList.size(); int i, j; if(m
	 * == 0) return diskList; if(n == 0) return memList;
	 * 
	 * List<String> mergedList = new ArrayList<String>(); for(i = 0, j = 0; i <
	 * m || j < n; ){ if(i >= m) mergedList.add(memList.get(i++)); else if(j >=
	 * n) mergedList.add(diskList.get(j++)); else{ //i < m && j < n String pairi
	 * = memList.get(i), pairj = diskList.get(j); String[] pair =
	 * pairi.split(":"); String doci = pair[0]; int cnti =
	 * Integer.parseInt(pair[1]); pair = pairj.split(":"); String docj =
	 * pair[0]; int cntj = Integer.parseInt(pair[1]); if(doci.compareTo(docj) <
	 * 0){ mergedList.add(pairi); i++; } else if(doci.compareTo(docj) > 0){
	 * mergedList.add(pairj); j++; } else{ int cnt = cnti + cntj; String
	 * mergedPair = doci + ":" + cnt; mergedList.add(mergedPair); i++; j++; } }
	 * }
	 * 
	 * return mergedList; }
	 */

}
