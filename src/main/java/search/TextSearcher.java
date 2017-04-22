package search;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.StringWriter;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class TextSearcher {

private final String SPLIT_KEY = " ";
private int threadSize = 0;
private List<Integer> wordCounts = new ArrayList<>();
private Map<Integer, CallableResult> resultMap;
private List<String> words = new ArrayList<>();


/**
 * Resolves the future passed as input. Builds a map with thread
 * position as key and the result as value
 *
 * @param future Future obtained from callable.
 * @returns m Map
 */
private Function<Future, Map<Integer, CallableResult>> extractFuture = future
        -> {
    try {
        CallableResult result = (CallableResult) future.get();
        Map<Integer, CallableResult> m = new HashMap<Integer, CallableResult>();
        m.put(result.getThreadPosition(), result);
        return m;
    } catch (Exception ex) {
        ex.printStackTrace();
        return null;
    }
};


public TextSearcher(File f) throws IOException {
    FileReader r = new FileReader(f);
    StringWriter w = new StringWriter();
    char[] buf = new char[4096];
    int readCount;

    while ((readCount = r.read(buf)) > 0) {
        w.write(buf, 0, readCount);
    }

    init(w.toString());
}

/**
 * Initializes any internal data structures that are needed for
 * this class to implement search efficiently.
 */
protected void init(String fileContents) {
    synchronized (this) {
        try {
            //Positions where the string can be split.
            List<Integer> positions = findBreakPoints(fileContents);

            // Number of threads = number of breakpoints -1.
            threadSize = positions.size() - 1;

            // A thread pool with the number of threads determined.
            ExecutorService executorService = Executors.newFixedThreadPool(threadSize);
            List<Callable<CallableResult>> callables = new ArrayList<>();

            //Initiates the thread pool with the filecontents, where to start,
            //where to end and the thread number.
            IntStream.range(0, threadSize).forEach(i ->
                    callables.add(callable(fileContents, positions.get(i),
                            positions.get(i + 1), i)));
            //Invokes all threads in parallel.
            //Stores the result as a map Map<Integer,CallableResult>
            /*Execution complexity:
                Best case: O(n)/number of threads. If the spring is split
                equally among all the threads
                worst case: O/n. If there is only one sentence in the string.
             */
            resultMap = executorService.invokeAll(callables).stream()
                    .map(extractFuture).reduce((m1, m2) -> {
                        m1.putAll(m2);
                        return m1;
                    }).orElse(new HashMap<>());
            int runningCount = 0;
            //Takes each word and puts it in an array.
            for (int i = 0; i < threadSize; i++) {
                runningCount = runningCount + resultMap.get(i).getWords().size();
                wordCounts.add(runningCount);
                words.addAll(resultMap.get(i).getWords());
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }
}
/**
 * Takes filecontents and splits it into different segments to be processed
 * by different threads without over lap. It tries to divide the thread into
 * 4 equal segments. But inorder to avoid split words, if the initial
 * position is not the end of a sentence, it iterates till it finds a fullstop.
 *
 * Note: This method has scope for improvement. instead of searching for the
 * end of the string, this method can be made to break at the end of a word.
 * However, for simplicity and lack of time, I have taken this approach.
 */
private List<Integer> findBreakPoints(String fileContents) {
    final int wordCount = fileContents.length();
    int size = wordCount / 4;
    List<Integer> positions = new ArrayList<>();
    //First thread always starts at 0
    positions.add(0);
    for (int i = 0; i < 4; i++) {
        int breakPoint = i < 3 ? fileContents.indexOf('.', size * i) + 1 :
                fileContents.length() - 1;
        positions.add(breakPoint);
    }
    System.out.println("positions = "+positions);
    return positions;

}

private Callable<CallableResult> callable(String textArr, int start, int end
        , int threadPosition) {
    return () -> {
        Map<String, Set<Integer>> stringPosition = new HashMap<>();
        List<String> words = new ArrayList<>();
        String word = "";
        boolean found = false;
        for (int i = start; i <= end; i++) {
            if (textArr.charAt(i) == ' ') {
                if (word.trim().length() < 1) {
                    word = word.concat(String.valueOf(textArr.charAt(i)));
                } else {
                    words.add(word);
                    found = true;
                }
            } else if (i == end) {
                word = word.concat(String.valueOf(textArr.charAt(i)));
                words.add(word);
                found = true;
            } else word = word.concat(String.valueOf(textArr.charAt(i)));
            if (found) {
                String str = word.replaceAll("\\s*\\p{Punct}+\\s*$", "")
                        .toLowerCase();
                Set<Integer> positions = stringPosition.get(str);
                if (positions == null) {
                    Set<Integer> pos = new HashSet<>();
                    pos.add(words.size() - 1);
                    stringPosition.put(str, pos);
                } else {
                    positions.add(words.size() - 1);
                    stringPosition.put(str, positions);
                }
                word = "";
                found = false;
            }
        }
        return new CallableResult(stringPosition, threadPosition, words);
    };
}

/**
 * @param queryWord    The word to search for in the file contents.
 * @param contextWords The number of words of context to provide on
 *                     each side of the query word.
 * @return One context string for each time the query word appears in the file.
 */
public String[] search(String queryWord, int contextWords) {
    Set<Integer> positions = new TreeSet<>();
    for (int i = 0; i < threadSize; i++) {
        final int threadPosition = i;
        CallableResult result = resultMap.get(i);
        Set<Integer> pos = result.getStringPosition().get(queryWord.toLowerCase());
        if (pos != null) {
            positions.addAll(i == 0 ? pos : pos.stream().map(p -> wordCounts.get(threadPosition - 1)
                    + p).collect(Collectors.toSet()));
        }
    }
    List<String> strings = (contextWords > 0) ? positions.stream().map(pos ->
            IntStream.rangeClosed(pos - contextWords, pos + contextWords).mapToObj
                    (p -> p >= 0 && p < words.size() ? words.get(p) : "").collect
                    (Collectors
                            .joining(" "))
    ).map(word -> word.trim().replaceAll(",$", ""))
            .collect(Collectors.toList()) :
            IntStream.range(0, positions.size()).mapToObj(x -> queryWord).collect
                    (Collectors.toList());
    return strings.toArray(new String[0]);
}

// Any needed utility classes can just go in this file

//The result returned by each thread.
class CallableResult {
    // <Lower cased words with special characters removed to seach easily ,
    //          Position in the words derived from the given set of characters>
    private final Map<String, Set<Integer>> stringPosition;
    // Words as is, dervied from the given string
    private final List<String> words;
    private final int threadPosition;

    CallableResult(Map<String, Set<Integer>> stringPosition, int threadPosition,
                   List<String> words) {
        this.stringPosition = stringPosition;
        this.threadPosition = threadPosition;
        this.words = words;
    }

    Map<String, Set<Integer>> getStringPosition() {
        return stringPosition;
    }

    int getThreadPosition() {
        return threadPosition;
    }

    public List<String> getWords() {
        return words;
    }

}
}