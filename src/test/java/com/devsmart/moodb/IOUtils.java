package com.devsmart.moodb;


import java.io.File;

public class IOUtils {

    public static void delete(File file) {
        for(File f : file.listFiles()){
            if(f.isDirectory()){
                delete(f);
            } else {
                f.delete();
            }
        }
        file.delete();
    }
}
