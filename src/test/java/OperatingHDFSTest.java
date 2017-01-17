import com.github.pwalan.OperatingHDFS;

import java.io.IOException;

/**
 * @author AlanP
 * @Date 2017/1/17
 */
public class OperatingHDFSTest {
    public static void main(String[] args) throws IOException {
        OperatingHDFS ofs = new OperatingHDFS();
        ofs.catFile("/user/alanp/input/test.txt");
    }
}
