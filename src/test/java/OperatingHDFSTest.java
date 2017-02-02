import com.github.pwalan.OperatingHDFS;
import org.junit.Test;
import java.io.IOException;

/**
 * @author AlanP
 * @date 2017/1/17
 */
public class OperatingHDFSTest {

    @Test
    public void testCatFile() throws IOException {
        OperatingHDFS ofs = new OperatingHDFS();
        //查看文件内容
        ofs.catFile("/user/alanp/input/test.txt");

    }

    @Test
    public void testPutFile() throws IOException{
        OperatingHDFS ofs = new OperatingHDFS();
        //将本地文件拷贝到hdfs上，C:\Users\pwala\Desktop
        ofs.copyFile("C:\\Users\\pwala\\Desktop\\test3.txt","/user/alanp/input");
    }


}
