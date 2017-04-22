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
        Map<Integer, CallableResult> m = new HashMap();
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

            //Initiates the thread pool with the file contents, where to start,
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
                System.out.println(resultMap.get(i).getWords());
                words.addAll(resultMap.get(i).getWords());
            }
            System.out.println(words);
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
 * <p>
 * TODO: This method has scope for improvement. instead of searching for the
 * end of the string, this method can be made to break at the end of a word.
 * However, for simplicity and lack of time, I have taken this approach.
 * Also, currently the thread size is fixed to 4. However, a smarter approach
 * would be to decide based on the number of cores available and the size of
 * the file.
 *
 * @Params: Filecontents
 * @Returns breakpoints
 */
private List<Integer> findBreakPoints(String fileContents) {
    final int wordCount = fileContents.length();
    int size = wordCount / 4;
    List<Integer> positions = new ArrayList<>();
    //First thread always starts at 0
    positions.add(0);
    for (int i = 0; i <= 3; i++) {
        // TODO: This needs to be improved.
        // Currently it looks for the first fullstop after the expected size
        // position.
        // In this case: If "www.google.com" comes after the expected size
        // position, it will break at www. But www.google.com is a single word.
        int breakPoint = i == 3 ? fileContents.length() - 1 : fileContents
                .indexOf(".", size * (i + 1)) + 1;
        if (breakPoint >= fileContents.length()) {
            positions.add(fileContents.length() - 1);
            break;
        }
        positions.add(breakPoint);
    }
    System.out.println(positions);
    return positions;

}

/**
 * Takes the whole text, start, end and thread postions as input.
 * It does two things:
 *      1) builds a key value pair, where key is each distinct string and
 *      value is the position of occurrance of the string with in the segment
 *      the current thread operates on. This strips all special characters
 *      and spaces.
 *      2) builds an array of words retaining special characters and spaces
 *
 * @Params: text, Input text
 * @param: start, start position from where the current thread should operate
 * @Params: end, the position till which the current string should operate
 * @Params: threadposition, the index of thread
 * @Returns future

 */


private Callable<CallableResult> callable(String text, int start, int end
        , int threadPosition) {
    return () -> {
        Map<String, Set<Integer>> stringPosition = new HashMap<>();
        List<String> words = new ArrayList<>();
        String word = "";
        boolean found = false;
        for (int i = start; i <= end; i++) {
            if (text.charAt(i) == ' ') {
                if (word.trim().length() < 1) {
                    // To eliminate the space added by list.addAll
                    word = i>start? word.concat(String.valueOf(text.charAt
                            (i))):"";
                } else {
                    words.add(word);
                    found = true;
                }
            } else if (i == end) {
                word = word.concat(String.valueOf(text.charAt(i)));
                words.add(word);
                found = true;
            } else word = word.concat(String.valueOf(text.charAt(i)));
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
    // Contains the positions of the occurrence of the words.
    Set<Integer> positions = new TreeSet<>();
    //Search individually in the result returned by each thread;
    for (int i = 0; i < threadSize; i++) {
        // Since only final can be used in lambdas.
        final int threadPosition = i;
        CallableResult result = resultMap.get(i);
        //Convert incoming query to lower case and search in the map.
        //The map returns the position of occurrence in the word array.
        Set<Integer> pos = result.getStringPosition().get(queryWord.toLowerCase());
        if (pos != null) {
            // Add to the positions list.
            positions.addAll(i == 0 ? pos : pos.stream().map(p -> wordCounts.get(threadPosition - 1)
                    + p).collect(Collectors.toSet()));
        }
    }
    // Fetch the words from the position and context postions
    List<String> strings = (contextWords > 0) ? positions.stream().map(pos ->
            IntStream.rangeClosed(pos - contextWords, pos + contextWords).mapToObj
                    (p -> p >= 0 && p < words.size() ?
                            words.get(p) : "").collect(Collectors.joining(" "))
    ).map(word -> word.trim().replaceAll(",$", ""))
            .collect(Collectors.toList()) :
            IntStream.range(0, positions.size()).mapToObj(x -> queryWord).collect
                    (Collectors.toList());
    // Convert it to an array and return.
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