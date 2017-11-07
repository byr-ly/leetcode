package com.eb.bi.rs.algorithm.pac.Util;

import com.eb.bi.rs.algorithm.pac.data.PCADataRow;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.Iterator;
import java.util.List;

public class ObjectWriter {
    private File fw = null;
    private Writer bw = null;

    public ObjectWriter(String path) {
        try {
            this.fw = new File(path);
            this.bw = new OutputStreamWriter(new FileOutputStream(this.fw), "gbk");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void close() {
        try {
            this.bw.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void write(List<PCADataRow> beans)
            throws IOException {
        for (Iterator localIterator = beans.iterator(); localIterator.hasNext(); ) {
            PCADataRow bean = (PCADataRow) localIterator.next();

            String line = bean.toString();
            this.bw.append(line);
            this.bw.append("\n");
        }
        this.bw.flush();
    }
}