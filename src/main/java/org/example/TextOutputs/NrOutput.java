package org.example.TextOutputs;

import org.apache.hadoop.io.Text;

public class NrOutput {
    private final long part;
    private final long r;
    private final long Nr;

    public NrOutput(Text line){
        //<part>\t<r>\t<Nr>
        String[] parts = line.toString().split("\\s");
        part = Integer.parseInt(parts[0]);
        r = Integer.parseInt(parts[1]);
        Nr = Integer.parseInt(parts[2]);
    }

    public long getPart() {
        return part;
    }

    public long getNr() {
        return Nr;
    }

    public long getR() {
        return r;
    }
}
