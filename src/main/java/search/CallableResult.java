package search;

import java.util.Map;
import java.util.Set;

/**
 * Created by vinodm1986 on 4/19/17.
 */
class CallableResult{
    private final Map<String,Set<Integer>> stringPosition;
    private final Map<Integer,String> positionString;
    private final int threadPosition;

    CallableResult(Map<String,Set<Integer>> stringPosition, Map<Integer,String> positionString,int threadPosition){
        this.positionString = positionString;
        this.stringPosition = stringPosition;
        this.threadPosition = threadPosition;
    }

    public Map<String, Set<Integer>> getStringPosition() {
        return stringPosition;
    }

    public Map<Integer, String> getPositionString() {
        return positionString;
    }

    public int getThreadPosition() {
        return threadPosition;
    }

    @Override
    public String toString() {
        return threadPosition+" "+ stringPosition.size()+" "+positionString.size();
    }
}