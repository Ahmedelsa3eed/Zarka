package org.zarka.sstable;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.zarka.utils.Formatter;

public class SSTable {
    private File baseFileName;

    public SSTable() {
        baseFileName = new File("data/segment-" + Formatter.getFileNameExtension());
    }

    public File getBaseFileName() {
        return baseFileName;
    }
}
