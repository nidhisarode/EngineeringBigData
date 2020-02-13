package putMerge;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

public class Driver {
    public static void main(String[] args) {
        try {
            //Creating a job
            Configuration conf = new Configuration();
            FileSystem hdfs = FileSystem.get(conf);

            //local File System instance
            FileSystem local = FileSystem.getLocal(conf);

            //Path to the directory in local file System
            Path inputDir = new Path(args[0]);
            Path hdfsFile = new Path(args[1]);

            try {
                //List of Files Names from FileStatus
                FileStatus[] inputFiles = local.listStatus(inputDir);
                //Writing to hdfs using OutputStream
                FSDataOutputStream out = hdfs.create(hdfsFile);

                //Reading the input files using FSDataInputStream
                for (int i = 0; i < inputFiles.length; i++) {
                    FSDataInputStream in = local.open(inputFiles[i].getPath());
                    byte buffer[] = new byte[256];
                    int bytesRead = 0;

                    while ((bytesRead = in.read(buffer)) > 0) {

                        out.write(buffer, 0, bytesRead);
                    }
                    in.close();
                }
                out.close();

            } catch (Exception e) {
                e.printStackTrace();
            }


        }catch (Exception e) {
            e.printStackTrace();
        }
    }

}