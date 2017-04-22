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
private List<Integer> wordCounts = new ArrayList<>();
int threadSize = 0;

Function<Future, Map<Integer, CallableResult>> extractFuture = future -> {
    try {
        CallableResult result = (CallableResult) future.get();
        Map m = new HashMap<Integer, CallableResult>();
        m.put(result.getThreadPosition(), result);
        return m;
    } catch (Exception ex) {
        ex.printStackTrace();
        return null;
    }
};
private Map<Integer, CallableResult> resultMap;
private List<String> words = new ArrayList<>();

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
            List<Integer> positions = findBreakPoints(fileContents);
            System.out.println("breakPoints = " + positions);
            threadSize = positions.size() - 1;
            ExecutorService executorService = Executors.newFixedThreadPool(threadSize);
            List<Callable<CallableResult>> callables = new ArrayList<>();
            IntStream.range(0, threadSize).forEach(i ->
                    callables.add(callable(fileContents, positions.get(i),
                            positions.get(i + 1), i)));
            resultMap = executorService.invokeAll(callables).stream()
                    .map(extractFuture).reduce((m1, m2) -> {
                        m1.putAll(m2);
                        return m1;
                    }).get();
            System.out.println(resultMap);
            int runningCount = 0;
            for (int i = 0; i < threadSize; i++) {
                runningCount = runningCount + resultMap.get(i).getWords().size();
                wordCounts.add(runningCount);
                words.addAll(resultMap.get(i).getWords());
            }
            System.out.println(wordCounts);
            System.out.println(words);
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }
}

private List<Integer> findBreakPoints(String fileContents) {
    final int wordCount = fileContents.length();
    int size = wordCount / 4;
    List<Integer> positions = new ArrayList<>();
    positions.add(0);
    for (int i = 0; i < 4; i++) {
        int breakPoint = fileContents.indexOf('.', size * i);
        breakPoint = breakPoint+1<fileContents.length()
                ?breakPoint+1:fileContents.length()-1;
        positions.add(breakPoint);
    }
    System.out.println("breakpoints = ");
    System.out.println(positions);
    return positions;

}

Callable<CallableResult> callable(String textArr, int start, int end
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
        System.out.println(words);
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
        Set<Integer> pos = result.getStringPosition().get(queryWord);
        if (pos != null) {
            positions.addAll(i == 0 ? pos : pos.stream().map(p -> wordCounts.get(threadPosition - 1)
                    + p).collect(Collectors.toSet()));
        }
    }
    System.out.println("positions = " + positions);
    List<String> strings = (contextWords > 0) ? positions.stream().map(pos ->
            IntStream.rangeClosed(pos - contextWords, pos + contextWords).mapToObj
                    (p -> words.get(p)).collect(Collectors.joining(" "))
    ).map(word->word.trim().replaceAll("\\s*\\p{Punct}+\\s*$", ""))
            .collect(Collectors.toList()) :
            IntStream.range(0, positions.size()).mapToObj(x -> queryWord).collect
                    (Collectors.toList());
    ;
    System.out.println(strings);
    return strings.toArray(new String[0]);
}

// Any needed utility classes can just go in this file

class CallableResult {
    private final Map<String, Set<Integer>> stringPosition;
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

    @Override
    public String toString() {
        String s = "";
        for (Map.Entry entry : stringPosition.entrySet()) {
            String key = (String) entry.getKey();
            Set<Integer> values = (Set<Integer>) entry.getValue();
            s = s.concat(key + "::" + values + System.lineSeparator());
        }
        s.concat(System.lineSeparator()+System.lineSeparator());
        return s;
    }
}
}