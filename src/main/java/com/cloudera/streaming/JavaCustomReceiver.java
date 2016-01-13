package com.cloudera.streaming;

import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.receiver.Receiver;

import java.io.File;
import java.util.*;

/**
 * Created by sergeyb on 11.01.16.
 */
public class JavaCustomReceiver extends Receiver<File> {

    String mDirectory = null;
    int port = -1;
    public JavaCustomReceiver(String directory) {
        super(StorageLevel.MEMORY_AND_DISK_2());
        mDirectory = directory;
    }

    public static Collection<File> recursiveListFiles(File dir) {
        Set<File> fileTree;
        fileTree = new HashSet<File>();
        System.out.println("recursive getting file list started time =" + System.currentTimeMillis());
        File[] listFiles = dir.listFiles();
        if (listFiles != null) {

            System.out.println("file list is not null size = " + listFiles.length);
            for (File entry : listFiles) {
                System.out.println("file name = " + entry.getName());
                if (entry.isFile()) fileTree.add(entry);
                else fileTree.addAll(recursiveListFiles(entry));
            }
        } else {
            System.out.println("file list is null");
        }
        System.out.println("recursive getting file list finished time =" + System.currentTimeMillis());
        System.out.println("listFiles size = " + fileTree.size());
        return fileTree;
    }

    public void onStart() {
        // Start the thread that receives data over a connection
        new Thread() {
            @Override
            public void run() {
                receive();
            }
        }.start();
    }

    public void onStop() {
        // There is nothing much to do as the thread calling receive()
        // is designed to stop by itself if isStopped() returns false
    }

    public List<File> ListFiles(File f) {
        List<File> files = Arrays.asList(f.listFiles());
        //TODO add recursive calling
        return files;
    }

    /**
     * Create a socket connection and receive data until receiver is stopped
     */
    private void receive() {
        store(recursiveListFiles(new File(mDirectory)).iterator());

    }


}