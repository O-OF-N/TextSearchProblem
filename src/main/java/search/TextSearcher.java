package search;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.StringWriter;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.Function;
import java.util.stream.IntStream;

public class TextSearcher {

	private Map<Integer,CallableResult> resultMap;
	private final String SPLIT_KEY = " ";
	int threadSize = 0;

	/**
	 * Initializes the text searcher with the contents of a text file.
	 * The current implementation just reads the contents into a string 
	 * and passes them to #init().  You may modify this implementation if you need to.
	 * 
	 * @param f Input file.
	 * @throws IOException
	 */
	public TextSearcher(File f) throws IOException {
		FileReader r = new FileReader(f);
		StringWriter w = new StringWriter();
		char[] buf = new char[4096];
		int readCount;
		
		while ((readCount = r.read(buf)) > 0) {
			w.write(buf,0,readCount);
		}
		
		init(w.toString());
	}
	
	/**
	 *  Initializes any internal data structures that are needed for
	 *  this class to implement search efficiently.
	 */
	protected void init(String fileContents) {
		synchronized (this) {

			try {
				final String[] words = fileContents.split(SPLIT_KEY);
				final int wordCount = words.length;
				threadSize = wordCount >= 4 ? 4 : wordCount;
				final int wordsPerThread = wordCount / threadSize;
				ExecutorService executorService = Executors.newFixedThreadPool(threadSize);
				List<Callable<CallableResult>> callables = new ArrayList<>();
				IntStream.range(0, threadSize).forEach(i -> {
					callables.add(callable(words, wordsPerThread * i, wordsPerThread * i + wordsPerThread, i));
				});
				resultMap = executorService.invokeAll(callables).stream().map(extractFuture).reduce((m1,m2)->{
					m1.putAll(m2);
					return m1;
				}).get();
				for(int i =0;i<threadSize;i++){
					System.out.println(resultMap.get(i));
				}
			} catch (Exception ex){
				ex.printStackTrace();
			}
		}
	}

	Function<Future,Map<Integer,CallableResult>> extractFuture = future-> {
		try {
			CallableResult result = (CallableResult)future.get();
			Map m = new HashMap<Integer, CallableResult>();
			m.put(result.getThreadPosition(), result);
			return m;
		} catch (Exception ex){
			ex.printStackTrace();
			return null;
		}
	};

	Callable<CallableResult> callable(String[] textArr, int start, int end, int threadPosition){
		return ()->{
			Map<String,Set<Integer>> stringPosition = new HashMap<>();
			Map<Integer,String> positionString = new HashMap<>();
			for(int i=start;i<=end;i++){
				positionString.put(i,textArr[i]);
				Set<Integer> positions = stringPosition.get(textArr[i]);
				if(positions == null){
					Set<Integer> pos = new HashSet<>();
					pos.add(i);
					stringPosition.put(textArr[i],pos);
				} else{
					positions.add(i);
					stringPosition.put(textArr[i],positions);
				}
			}
			return new CallableResult(stringPosition,positionString,threadPosition);
		};
	}
	
	/**
	 * 
	 * @param queryWord The word to search for in the file contents.
	 * @param contextWords The number of words of context to provide on
	 *                     each side of the query word.
	 * @return One context string for each time the query word appears in the file.
	 */
	public String[] search(String queryWord,int contextWords) {
		Set<Integer> positions = new HashSet<>();
		for(int i=0;i<threadSize;i++){
			CallableResult result = resultMap.get(i);
			Set<Integer> pos = result.getStringPosition().get(queryWord);
			if(pos!=null){
				positions.addAll(pos);
			}
		}
		positions.forEach(System.out::print);
		return new String[0];
	}
}

// Any needed utility classes can just go in this file

