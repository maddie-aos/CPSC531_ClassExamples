import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.hdfs.client.HdfsDataInputStream;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.yarn.api.records.URL;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.URI;
import java.util.List;

public class TestFileLevelAPI {

    public static final Log log = LogFactory.getLog(TestFileLevelAPI.class);

    public void createFile(String fileName) throws Exception {
        //
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        Path path = new Path(fileName);
        FSDataOutputStream out = fs.create(path);
        //
        BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(out));
        log.info("Start writing data to the file. ");
        for (int i = 0; i < 15; i++) {
            bw.write("New Line " + i);
            bw.newLine();
        }
        log.info("End writing data to the file. ");
        bw.close();
    }

    public void getMetadata(String fName) throws Exception {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        Path path = new Path(fName);
        FileStatus fStatus = fs.getFileStatus(new Path(fName));
        log.info("Owner of the file : " + fStatus.getOwner());
        log.info("File Len : " + fStatus.getLen());
        log.info("File Block Size : " + fStatus.getBlockSize());
        log.info("File Group: " + fStatus.getGroup());
        log.info("File Permission : " + fStatus.getPermission());
        log.info("File Access Time : " + fStatus.getAccessTime());
        log.info("File Modification Time : " + fStatus.getModificationTime());
    }

    public void getMetadataForDirectory(String fName) throws Exception {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        Path path = new Path(fName);
        String pathOnHadoop = fName;
        if (fName.equalsIgnoreCase("/"))
            pathOnHadoop = fs.getWorkingDirectory() + "/";
        //
        FileStatus[] fStatus = fs.listStatus(new Path(pathOnHadoop));
        for (FileStatus st : fStatus) {
            log.info("File Path Name : " + st.getPath().toString());
            log.info("Owner of the file : " + st.getOwner());
            log.info("File Len : " + st.getLen());
            log.info("File Block Size : " + st.getBlockSize());
            log.info("File Group: " + st.getGroup());
            log.info("File Permission : " + st.getPermission());
            log.info("File Access Time : " + st.getAccessTime());
            log.info("File Modification Time : " + st.getModificationTime());
        }
    }

    public void getBlockInfo(String fName) throws Exception {
        Configuration conf = new Configuration();
        Path path = new Path(fName);
        URI hostURI = path.toUri();
        log.info("URI: " + hostURI.getHost());
        log.info("URI: " + hostURI.getPort());
        log.info("URI: " + hostURI.getRawPath());
        FileSystem fs = FileSystem.get(conf);
        HdfsDataInputStream hdis = (HdfsDataInputStream) fs.open(path);
        List<LocatedBlock> lblocks = hdis.getAllBlocks();
        log.info("The number of blocks in the file: " + lblocks.size());
        for (LocatedBlock lblk : lblocks) {
            log.info("The Start Offset: " + lblk.getStartOffset());
            log.info("The Block Size: " + lblk.getBlockSize());
            log.info("The Block Type: " + lblk.getBlockType());
            log.info("The Block Token: " + lblk.getBlockToken());
            log.info("The Datanode Name: " + lblk.getLocations()[0].getHostName());
            ExtendedBlock eb = lblk.getBlock();
            log.info("The Block Name: " + eb.getBlockName());
            log.info("The Block Pool ID: " + eb.getBlockPoolId());
            log.info("The Block ID: " + eb.getBlockId());
            Block lb = eb.getLocalBlock();
            log.info("Number of Bytes: " + lb.getNumBytes());
        }

    }

    public void readFile(String fileName) throws Exception {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        Path path = new Path(fileName);
        FSDataInputStream in = fs.open(path);
        in.seek(0);     // Position back to start of the file
        InputStreamReader reader = new InputStreamReader(in);
        BufferedReader br = new BufferedReader(reader);
        String line = null;
        while ((line = br.readLine()) != null) {
            log.info(line);
        }
        //
        BlockLocation[] locs = fs.getFileBlockLocations(path, 0, 1000);
        log.info("Number of blocks: " + locs.length);
        for (BlockLocation b : locs) {
            log.info("Block offset: " + b.getOffset());
            log.info("Block Length: " + b.getLength());
            log.info("Datanode: " + b.getNames()[0]);
        }
        // FileStatus - Metadata
        FileStatus fStatus = fs.getFileStatus(path);
        log.info("File Block Size: " + fStatus.getBlockSize());
        log.info("File content Size: " + fStatus.getLen());
        log.info("File Group String: " + fStatus.getGroup());
        log.info("File Owner String: " + fStatus.getOwner());
        in.close();
    }

    public static void main(String[] args) throws Exception {
        //new TestFileLevelAPI().readFile(args[0]);
        //new TestFileLevelAPI().getMetadata(args[0]);
        //new TestFileLevelAPI().getMetadataForDirectory(args[0]);
        new TestFileLevelAPI().getBlockInfo(args[0]);
    }
}
