// cc FileSystemCat Displays files from a Hadoop filesystem on standard
// output by using the FileSystem directly
//import java.io.InputStream;

import java.net.URI;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.io.IOUtils;

// vv FileSystemCat
public class FileSystemCat {

    public static void main(String[] args) throws Exception {
        String uri = args[0];
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(URI.create(uri), conf);
        FSDataInputStream in = null;

        Path path = new Path(uri);

        in = fs.open(path);
        IOUtils.copyBytes(in, System.out, 4096, true);

    }
}
// ^^ FileSystemCat