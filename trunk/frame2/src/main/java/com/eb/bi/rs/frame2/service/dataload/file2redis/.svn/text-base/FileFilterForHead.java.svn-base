package com.eb.bi.rs.frame2.service.dataload.file2redis;

import java.io.File;
import java.io.FileFilter;


/**
 * @author zzl
 */

public class FileFilterForHead implements FileFilter {

    private String condition = "";

    public FileFilterForHead(String condition) {

        this.condition = condition;
    }

    public boolean accept(File pathname) {
        String filename = pathname.getName();
        if (filename.startsWith(condition)) {
            return true;
        } else
            return false;
    }
}
