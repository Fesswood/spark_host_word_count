package com.cloudera.streaming;

import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.receiver.Receiver;
import scala.collection.mutable.ArrayBuffer;
import scala.collection.mutable.StringBuilder;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;

/**
 * Created by sergeyb on 11.01.16.
 */
public class JavaCustomReceiver extends Receiver<String> {

    String mDirectory = null;
    int port = -1;

    public JavaCustomReceiver(String directory) {
        super(StorageLevel.MEMORY_AND_DISK_2());
        mDirectory = directory;
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

    public static Collection<File> recursiveListFiles(File dir) {
        Set<File> fileTree;
        fileTree = new HashSet<File>();
        for (File entry : dir.listFiles()) {
            if (entry.isFile()) fileTree.add(entry);
            else fileTree.addAll(recursiveListFiles(entry));
        }
        return fileTree;
    }

    /**
     * Create a socket connection and receive data until receiver is stopped
     */
    private void receive() {

        ArrayList<String> buffer=new ArrayList<>();
        for (File file : recursiveListFiles(new File(mDirectory))) {
            try(BufferedReader br = new BufferedReader(new FileReader(file))) {
                for(String line; (line = br.readLine()) != null; ) {
                    // process the line.
                    if (line.contains("TIME_STAMP")) {
                        continue;
                    }
                    buffer.add(line);
                }
                store(buffer.iterator());
                // line is not visible here.
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
       /* Socket socket = null;
        String userInput = null;

        try {
            // connect to the server
            socket = new Socket(host, port);

            BufferedReader reader = new BufferedReader(new InputStreamReader(socket.getInputStream()));

            // Until stopped or connection broken continue reading
            while (!isStopped() && (userInput = reader.readLine()) != null) {
                System.out.println("Received data '" + userInput + "'");
                store(userInput);
            }
            reader.close();
            socket.close();

            // Restart in an attempt to connect again when server is active again
            restart("Trying to connect again");
        } catch(ConnectException ce) {
            // restart if could not connect to server
            restart("Could not connect", ce);
        } catch(Throwable t) {
            // restart if there is any other error
            restart("Error receiving data", t);
        }*/
    }
}