
import org.apache.hadoop.conf.Configuration;
import org.apage.hadoop.fs.filesystem;
import org.apache.hadoop.fs.Path;
 




public class TestFileAPI
{
    public void createFile(string fileName) throws Exception
    {
        //
        Configuration conf = new Configuration ();
        FileSystem fs FileSystem.get(conf);
        Path path = new Path(fileName);
        FSDataOutputStream out= fs.create(path);
        //

        BufferedWriter bw= BufferedWriter(new OutputStreamWriter(out));
        for (int i =0; i <10; i++)
        {
            bw.write(str. "New Line" +i );
            bw.newLine();
        }
        bw.close();

    }

    public static void main(String[] args) throws Exception 
    {
        new TestFileLevelAPI().createFile(fileName "TestFile.txt");
    }
}
