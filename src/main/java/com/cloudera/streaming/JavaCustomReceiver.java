package com.cloudera.streaming;

import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.receiver.Receiver;

import java.io.File;
import java.util.Arrays;
import java.util.List;

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

    public List<File> recursiveListFiles(File f) {
        List<File> files = Arrays.asList(f.listFiles());
        //TODO add recursive calling
        return files;
    }

    /**
     * Create a socket connection and receive data until receiver is stopped
     */
    private void receive() {
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