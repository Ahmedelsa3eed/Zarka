package org.zarka.sstable;

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;


import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.zarka.avro.WeatherData;
import org.zarka.model.Memtable;

public class SSTable {
    private File baseFileName;
    private static Logger logger = LogManager.getLogger(SSTable.class);

    public SSTable() {
        baseFileName = new File("data/segment-" + getFileNameExtension());
    }

    private String getFileNameExtension() {
        Date date = new Date(System.currentTimeMillis());
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd-HH-mm-ss");
        return dateFormat.format(date);
    }

    public void write(Memtable memtable) {
        logger.info("Writing memtable to SSTable");
        try {
            DataOutputStream dos = new DataOutputStream(
                new FileOutputStream(baseFileName, true));
            for (WeatherData data: memtable.getInOrder())
                dos.write(data.toByteBuffer().array());
            dos.flush();
            dos.close();
        }
        catch (FileNotFoundException e) {
            logger.error("SSTable directory not found: ", e);
        }
        catch (IOException e) {
            logger.error("Error writing to SSTable: ", e);
        }
    }

    public String getSegmentPath() {
        return baseFileName.getAbsolutePath();
    }
}
