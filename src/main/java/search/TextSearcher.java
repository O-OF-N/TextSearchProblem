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
private String[] words;

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
            words = fileContents.split(SPLIT_KEY);
            final int wordCount = words.length;
            threadSize = wordCount >= 4 ? 4 : wordCount;
            final int wordsPerThread = wordCount / threadSize;
            ExecutorService executorService = Executors.newFixedThreadPool(threadSize);
            List<Callable<CallableResult>> callables = new ArrayList<>();
            IntStream.range(0, threadSize).forEach(i -> {
                callables.add(callable(words, wordsPerThread * i, wordsPerThread * i + wordsPerThread, i));
            });
            resultMap = executorService.invokeAll(callables).stream().map(extractFuture).reduce((m1, m2) -> {
                m1.putAll(m2);
                return m1;
            }).get();
            for (int i = 0; i < threadSize; i++) {
                System.out.println(resultMap.get(i));
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }
}

Callable<CallableResult> callable(String[] textArr, int start, int end
        , int threadPosition) {
    return () -> {
        Map<String, Set<Integer>> stringPosition = new HashMap<>();
        for (int i = start; i <= end; i++) {
            String str =textArr[i].replaceAll("\\s*\\p{Punct}+\\s*$", "").toLowerCase();
            Set<Integer> positions = stringPosition.get(str);
            if (positions == null) {
                Set<Integer> pos = new HashSet<>();
                pos.add(i);
                stringPosition.put(str, pos);
            } else {
                positions.add(i);
                stringPosition.put(str, positions);
            }
        }
        return new CallableResult(stringPosition, threadPosition);
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
        CallableResult result = resultMap.get(i);
        Set<Integer> pos = result.getStringPosition().get(queryWord);
        if (pos != null) {
            positions.addAll(pos);
        }
    }
    System.out.println("positions = "+positions);
    List<String> strings = positions.stream().map(pos ->
            IntStream.rangeClosed(pos - contextWords, pos + contextWords).mapToObj
                    (p -> words[p])
                    .collect(Collectors.joining(" "))
    ).collect(Collectors.toList());
    System.out.println(strings);
    return strings.toArray(new String[0]);
}
}

// Any needed utility classes can just go in this file

class CallableResult {
private final Map<String, Set<Integer>> stringPosition;
private final int threadPosition;

CallableResult(Map<String, Set<Integer>> stringPosition, int threadPosition) {
    this.stringPosition = stringPosition;
    this.threadPosition = threadPosition;
}

Map<String, Set<Integer>> getStringPosition() {
    return stringPosition;
}

int getThreadPosition() {
    return threadPosition;
}

@Override
public String toString() {
    String s = "";
    for (Map.Entry entry : stringPosition.entrySet()) {
        String key = (String) entry.getKey();
        Set<Integer> values = (Set<Integer>) entry.getValue();
        s = s.concat(key + "::" + values + System.lineSeparator());
    }
    return s;
}
}