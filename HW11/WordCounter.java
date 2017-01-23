import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.io.BufferedReader;

public class WordCounter {

	String delimiters = "[\\s\\t,;\\.\\?!-:@\\[\\]\\(\\)\\{\\}_\\*/=\\\"'\\d\\+\\^\\$]+";
	
	private List<HashMap<String, Integer>> mapper(String filename) {
		File file = new File(filename);
		
		String line = null;
		List<HashMap<String, Integer>> wordList = new LinkedList<HashMap<String, Integer>>();
		
		try {
			// Read the input file.
			BufferedReader br = new BufferedReader(new FileReader(file));
					
			// Divide each sentence into a list of words.
			while ((line = br.readLine()) != null) {
				String[] words = line.split(delimiters);

				for (String word : words) {
					// Remove leading and trailing whitespace.
					word = word.trim();
					// Covert the word to lower case
					word = word.toLowerCase();
					
					if (word.isEmpty()) {
						continue;
					}
					
					// Produce the name value pairs, where the name is the word itself,
					// and the value is 1. Like (word, 1)
					HashMap<String, Integer> pair = new HashMap<String, Integer>();
					pair.put(word, 1);
					
					wordList.add(pair);
				}
			}
			
			br.close();
			
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		return wordList;
	}
	
	private List<Map.Entry<String, Integer>> reducer(List<HashMap<String, Integer>> wordList) {
		HashMap<String, Integer> reducedPairs = new HashMap<String, Integer>();
		
		// Take the (word, 1) pairs and aggregate it into the word counts (word, count).
		for (int i = 0; i < wordList.size(); i++) {
			HashMap<String, Integer> pair = wordList.get(i);
			// Each HashMap only have one entry here. Get the key value pair.
			Entry<String, Integer> entry = pair.entrySet().iterator().next();
			
			String word = entry.getKey();
			int val = entry.getValue();
			
			// Aggregate to (word, count) pairs.
			if (reducedPairs.containsKey(word)) {
				reducedPairs.put(word, reducedPairs.get(word) + val);				
			} else {
				reducedPairs.put(word, val);
			}
		}
		
		// Sort all the entries based on its occurence. If the words have the same occurance,
		// sort them alphabetically.
		List<Map.Entry<String, Integer>> sortedEntries = sortHashMap(reducedPairs);
        
		return sortedEntries;
	}
	
	// Sort the name value pairs.
	private List<Map.Entry<String, Integer>> sortHashMap(HashMap<String, Integer> hashmap) {
        List<Map.Entry<String, Integer>> sortedEntries = new LinkedList<Map.Entry<String, Integer>>(hashmap.entrySet());
        
        Collections.sort(sortedEntries, new Comparator<Map.Entry<String, Integer>>() {
        	public int compare(Map.Entry<String, Integer> left, Map.Entry<String, Integer> right) {
        		if (left.getValue() != right.getValue()) {
        			return right.getValue() - left.getValue();
        		}
        		
        		return left.getKey().compareTo(right.getKey());
        	}
        });
        
        return sortedEntries;
	}

	private void printHashMapList(List<HashMap<String, Integer>> list) {
		for (int i = 0; i < list.size(); i++) {
			HashMap<String, Integer> hashmap = list.get(i);
			Iterator<Entry<String, Integer>> iterator = hashmap.entrySet().iterator();
			while (iterator.hasNext()) {
				printHashMapEntry(iterator.next());
			}
		}
	}
	
	private void printHashEntryList(List<Map.Entry<String, Integer>> list) {
		for (int i = 0; i < list.size(); i++) {
			printHashMapEntry(list.get(i));
		}
	}
	
	private void printHashMapEntry(Map.Entry<String, Integer> entry) {
		System.out.print("(" + entry.getKey() + ", " + entry.getValue() + ") ");
	}
	
	public static void main (String[] args) {
		String filename = "/Users/hqiu/Documents/workspace/WordCounter/src/input.txt";
		
		WordCounter wordCounter = new WordCounter();
		
		List<HashMap<String, Integer>> wordList = wordCounter.mapper(filename);
		System.out.println("The output pairs from the Mapper function are: \n");
		wordCounter.printHashMapList(wordList);
		
		List<Map.Entry<String, Integer>> sortedEntries = wordCounter.reducer(wordList);
		System.out.println("\n\nThe output pairs from the Reducer function are: \n");
		wordCounter.printHashEntryList(sortedEntries);
	}
}

