import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.IOException;
import java.net.URI;

public class Solution1 {
    public static void main(String [] args) throws IOException {
        /*
        Use the applications FileSystemCat.java and FileSystemPut.java to
implement in Java HDFS application, that moves a file from one location in HDFS into
another location in HDSF.
The application must have the following two parameters.
(1) A path to and a name of file to be moved from.
(2) A path to and a new name of file to be moved to.
         */
        args = new String[]{"/Users/home/Documents/Big Data Management/src/Tester.txt", "output.txt"};
        String localStr = args[0];
        String hdfsStr = args[1];
        Configuration conf = new Configuration();
        FileSystem local = FileSystem.getLocal(conf);
        FileSystem hdfs = FileSystem.get(URI.create(hdfsStr), conf);
        Path localFile = new Path(localStr);
        Path hdfsFile = new Path(hdfsStr);
        FSDataInputStream in = local.open(localFile);
        FSDataOutputStream out = hdfs.create(hdfsFile);
        IOUtils.copyBytes(in, out, 4096, true);

    }
}
