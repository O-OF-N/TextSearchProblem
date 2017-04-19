package search;

import java.util.Iterator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Iterator class which returns token strings from the input string.  
 * It must be initialized with a regular expression representing a word.
 * Returned tokens will alternate between words (i.e. that match the regular expression)
 * and intervening strings that are not words.<p>
 * 
 * The method isWord(String) can be used to check whether a given token is a word
 * or an intervening token.
 * 
 * To learn what regular expression syntax is supported by the Java platform,
 * see the documentation for java.util.regex.Pattern.
 */
public class TextTokenizer implements Iterator<String> {

	private String input;
	private Pattern wordPattern;
	private Matcher matcher;
	private int prevWordEnd;
	private String nextPunctuation;
	private String nextWord;
	
	/**
	 * Initializes the tokenizer with an input string and a regular expression
	 * representing a word.
	 * 
	 * @param input
	 * @param wordRegex
	 */
	public TextTokenizer(String input,String wordRegex) {
		this.input = input;
		
		this.wordPattern = Pattern.compile(wordRegex);
		this.matcher = wordPattern.matcher(input);
	}
	
	/** Returns true if more tokens are available, i.e. if end-of-file has
	 *  not been reached.
	 */
	public boolean hasNext() {
		if (nextPunctuation == null && nextWord == null) {
			retrieveNext();
		}
		return (nextPunctuation != null || nextWord != null);
	}

	/** Returns the next token if any more tokens are available.  Otherwise returns null. */
	public String next() {
		if (nextPunctuation == null && nextWord == null) {
			retrieveNext();
		}
		
		String result;
		if (nextPunctuation != null) {
			result = nextPunctuation;
			nextPunctuation = null;
		}
		else {
			result = nextWord;
			nextWord = null;
		}
		
		return result;
	}

	/** Unsupported operation. */
	public void remove() {
		throw new UnsupportedOperationException();
	}
	
	/**
	 * Returns true if the string matches this tokenizer's regular expression, otherwise
	 * returns false.
	 */
	public boolean isWord(String s) {
		return wordPattern.matcher(s).matches();
	}
	
	private void retrieveNext() {
		if (matcher == null) return;
		if (matcher.find()) {
			int wordStart = matcher.start();
			int wordEnd = matcher.end();
			
			nextWord = input.substring(wordStart,wordEnd);
			if (wordStart > prevWordEnd) {
				nextPunctuation = input.substring(prevWordEnd,wordStart);
			}
			
			prevWordEnd = wordEnd;
		} 
		else {
			if (prevWordEnd < input.length()) {
				nextPunctuation = input.substring(prevWordEnd,input.length());
				prevWordEnd = input.length();
			}
			matcher = null;
		}
	}

}
