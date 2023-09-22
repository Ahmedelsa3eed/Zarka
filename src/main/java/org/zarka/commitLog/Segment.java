package org.zarka.commitLog;

import java.io.File;

public class Segment {
    private String segmentName;
    private File logFile;
    private boolean active;

    public Segment(String segmentName) {
        this.segmentName = segmentName;
        this.logFile = new File(segmentName);
        this.active = false;
    }

    public String getSegmentName() {
        return segmentName;
    }

    public boolean isActive() {
        return active;
    }

    public void setActive() {
        this.active = true;
    }
}
