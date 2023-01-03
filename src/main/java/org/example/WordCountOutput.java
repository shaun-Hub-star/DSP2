package org.example;

import org.apache.hadoop.io.Text;

public class WordCountOutput {

    private final long count;
    private final Text text;


    public WordCountOutput(Text line) {
        //line: <part>\t<ngram>\t<count>
        String[] parts = line.toString().split("\\t");
        this.count = Integer.parseInt(parts[2]); //.replace(" ","")
        this.text = new Text("" + count);
    }

    public long getCount() {
        return count;
    }

    public Text getText() {
        return text;
    }
}
