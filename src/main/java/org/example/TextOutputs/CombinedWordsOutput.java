package org.example.TextOutputs;

import org.apache.hadoop.io.Text;

public class CombinedWordsOutput {

    private final long count0;
    private final long count1;
    private final Text words;

    public CombinedWordsOutput(Text line) {
        String[] parts = line.toString().split("\\t");
        words = new Text(parts[0]);
        count0 = Integer.parseInt(parts[1]);
        count1 = Integer.parseInt(parts[2]);

    }

    public long getCount1() {
        return count1;
    }

    public long getCount0() {
        return count0;
    }

    public Text getWords() {
        return words;
    }
}
