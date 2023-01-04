package org.example.TextOutputs;

import org.apache.hadoop.io.Text;

@Deprecated
public class WordCountOutput {

    private final long part;
    private final long count;
    private final Text words;


    public WordCountOutput(Text line) {
        //line: <part>\t<ngram>\t<count>
        String[] parts = line.toString().split("\\t");
        this.part = Integer.parseInt(parts[0]);
        this.words = new Text(parts[1]);
        this.count = Integer.parseInt(parts[2]);
    }

    public long getCount() {
        return count;
    }

    public Text getWords(){
        return words;
    }

    public long getPart() {
        return part;
    }
}
