package info.goodline.Utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import java.io.IOException;
import java.util.Arrays;

public class PutMerge {

    private static final int TREAD_COUNT = 4;
    public static volatile int treadFinishedCount = TREAD_COUNT;

    public static void main(String[] args) {
        // TODO Auto-generated method stub
        // Задаем входной
        //каталог и выходной файл
        //Получаем список
        try {
            long timeBefore = System.currentTimeMillis();
            System.out.println("Start Time is " + (timeBefore) / 1000);
            Configuration conf = new Configuration();
            FileSystem hdfs = FileSystem.get(conf);
            FileSystem local = FileSystem.getLocal(conf);
            Path inputDir = new Path(args[0]);
            Path hdfsFile = new Path(args[1]);
            System.out.println("input path:" + inputDir.toString());
            System.out.println("input hdfs path:" + hdfsFile.toString());
            //локальных файлов
            FileStatus[] inputFiles = local.listStatus(inputDir);
            FileStatus[][] splitFiles = new FileStatus[4][];
            int partSize = inputFiles.length / TREAD_COUNT;
            int lastSize = partSize + inputFiles.length % TREAD_COUNT;
            System.out.println("partSize " + partSize);
            System.out.println("lastSize " + lastSize);
            for (int i = 0; i < TREAD_COUNT - 1; i++) {
                splitFiles[i] = Arrays.copyOfRange(inputFiles, i * partSize, (i + 1) * partSize);
            }
            splitFiles[3] = Arrays.copyOfRange(inputFiles, 3 * lastSize, 4 * partSize);


            for (int i = 0; i < TREAD_COUNT; i++) {
                System.out.println("create tread");
                WriteHadoopRunnable hr = new WriteHadoopRunnable(i, conf, inputFiles, hdfsFile.suffix(".part" + i));
                System.out.println("start tread");
                new Thread(hr).start();
            }
        /*FSDataOutputStream out = hdfs .create (hdfsFile) ;
				//Создаем поток 
				for (int i=0; i<inputFiles.length;i++){
					System.out.println(inputFiles[i].getPath().getName());
					FSDataInputStream in =
					 local.open(inputFiles[i] .getPath()) ; 
					byte buffer[] = new byte[256];
					int bytesRead = 0;
					while( (bytesRead = in.read(buffer)) > 0 ){
						out.write(buffer, 0, bytesRead);
					}
					in.close();
				}
				out.close();*/
            while (treadFinishedCount > 0) {

            }
            long timeAfter = System.currentTimeMillis();
            System.out.println("Start Time is " + (timeBefore) / 1000);
            System.out.println("End Time is " + (timeBefore) / 1000);
            System.out.println("Execution Time is " + (timeAfter - timeBefore) / 1000);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static class WriteHadoopRunnable implements Runnable {

        FileSystem local;
        int number;
        private Configuration conf;
        private Path hdfsFile;
        private FileStatus[] inputFiles;

        public WriteHadoopRunnable(int number, Configuration conf, FileStatus[] inputFiles, Path hdfsFile) throws IOException {
            this.conf = conf;
            this.hdfsFile = hdfsFile;
            this.inputFiles = inputFiles;
            this.number = number;
            local = FileSystem.getLocal(conf);
        }

        @Override
        public void run() {
            try {
                FileSystem hdfs = FileSystem.get(conf);
                FSDataOutputStream out = hdfs.create(hdfsFile);
                //Создаем поток
                for (int i = 0; i < inputFiles.length; i++) {
                    System.out.println("Tread " + number + " write " + inputFiles[i].getPath().getName());
                    FSDataInputStream in =
                            local.open(inputFiles[i].getPath());
                    byte buffer[] = new byte[256];
                    int bytesRead = 0;
                    while ((bytesRead = in.read(buffer)) > 0) {
                        out.write(buffer, 0, bytesRead);
                    }
                    in.close();
                }
                out.close();
                treadFinishedCount = treadFinishedCount - 1;
            } catch (IOException e) {
                e.printStackTrace();
                treadFinishedCount = treadFinishedCount - 1;
            }
        }

    }

}
