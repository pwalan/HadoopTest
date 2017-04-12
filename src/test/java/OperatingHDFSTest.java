import com.github.pwalan.hdfs.OperatingHDFS;
import org.junit.Test;
import java.io.IOException;

/**
 * @author AlanP
 * @date 2017/3/19
 */
public class OperatingHDFSTest {

    @Test
    public void testCreateDir() throws IOException{
        OperatingHDFS ofs = new OperatingHDFS();
        //创建目录
        ofs.createDir("/user/alanp/input");
    }

    @Test
    public void testPutFile() throws IOException{
        OperatingHDFS ofs = new OperatingHDFS();
        //将本地文件拷贝到hdfs上，C:\Users\pwala\Desktop
        ofs.copyFile("C:\\Users\\pwala\\Desktop\\test.txt","/user/alanp/input");
    }

    @Test
    public void testCreateFile() throws IOException{
        OperatingHDFS ofs = new OperatingHDFS();
        //创建新文件
        ofs.createFile("/user/alanp/input/hello.txt","Hello World!");
    }

    @Test
    public void testAppendFile() throws IOException{
        OperatingHDFS ofs = new OperatingHDFS();
        //在文件后追加内容
        ofs.appendFile("/user/alanp/input/hello.txt","I'm a student.");
    }

    @Test
    public void testListFiles() throws IOException{
        OperatingHDFS ofs = new OperatingHDFS();
        //列出目录下所有文件及文件夹
        ofs.listFiles("/user/alanp/input");
    }

    @Test
    public void testCatFile() throws IOException {
        OperatingHDFS ofs = new OperatingHDFS();
        //查看文件内容
        ofs.catFile("/user/alanp/input/test.txt");

    }

    @Test
    public void testDeleteFile() throws IOException{
        OperatingHDFS ofs = new OperatingHDFS();
        //删除文件
        ofs.deleteFile("/user/alanp/input/test.txt");
    }


}
