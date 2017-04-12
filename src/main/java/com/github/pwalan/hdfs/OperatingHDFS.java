package com.github.pwalan.hdfs;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.security.PrivilegedExceptionAction;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.security.UserGroupInformation;


/**
 * 远程操作HDFS
 *
 * @author AlanP
 * @Date 2017/3/19
 */
public class OperatingHDFS {
    public static String HDFS_URL="hdfs://192.168.213.128:9000/";
    public static String USERNAME="alanp";
    /**
     * 静态初始化
     */
    static Configuration conf = new Configuration();
    static FileSystem hdfs;

    static {
        UserGroupInformation ugi = UserGroupInformation
                .createRemoteUser("alanp");
        try {
            ugi.doAs(new PrivilegedExceptionAction<Void>() {
                public Void run() throws Exception {
                    Configuration conf = new Configuration();
                    conf.set("fs.defaultFS", HDFS_URL);
                    conf.set("hadoop.job.ugi", USERNAME);
                    Path path = new Path(HDFS_URL);
                    hdfs = FileSystem.get(path.toUri(), conf);
                    return null;
                }
            });
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    /**
     * 创建目录
     *
     * @param dir
     * @throws IOException
     */
    public void createDir(String dir) throws IOException {
        Path path = new Path(dir);
        if (hdfs.exists(path)) {
            System.out.println("dir \t" + conf.get("fs.default.name") + dir
                    + "\t already exists");
            return;
        }
        hdfs.mkdirs(path);
        System.out.println("new dir \t" + conf.get("fs.default.name") + dir);
    }

    /**
     * 将本地文件拷贝到hdfs上
     *
     * @param localSrc
     * @param hdfsDst
     * @throws IOException
     */
    public void copyFile(String localSrc, String hdfsDst) throws IOException {
        Path src = new Path(localSrc);
        Path dst = new Path(hdfsDst);
        if (!(new File(localSrc)).exists()) {
            System.out.println("Error: local dir \t" + localSrc
                    + "\t not exists.");
            return;
        }
        if (!hdfs.exists(dst)) {
            System.out.println("Error: dest dir \t" + dst.toUri()
                    + "\t not exists.");
            return;
        }
        String dstPath = dst.toUri() + "/" + src.getName();
        if (hdfs.exists(new Path(dstPath))) {
            System.out.println("Warn: dest file \t" + dstPath
                    + "\t already exists.");
        }
        hdfs.copyFromLocalFile(src, dst);
        // list all the files in the current direction
        FileStatus files
                [] = hdfs.listStatus(dst);
        System.out.println("Upload to \t" + conf.get("fs.default.name")
                + hdfsDst);
        for (FileStatus file : files) {
            System.out.println(file.getPath());
        }
    }

    /**
     * 创建新文件
     *
     * @param fileName
     * @param fileContent
     * @throws IOException
     */
    public void createFile(String fileName, String fileContent)
            throws IOException {
        Path dst = new Path(fileName);
        byte[] bytes = fileContent.getBytes();
        FSDataOutputStream output = hdfs.create(dst);
        output.write(bytes);
        System.out.println("new file \t" + conf.get("fs.default.name")
                + fileName);
    }

    /**
     * 在文件后追加内容
     *
     * @param fileName
     * @param fileContent
     * @throws IOException
     */
    public void appendFile(String fileName, String fileContent)
            throws IOException {
        Path dst = new Path(fileName);
        byte[] bytes = fileContent.getBytes();
        if (!hdfs.exists(dst)) {
            createFile(fileName, fileContent);
            return;
        }
        FSDataOutputStream output = hdfs.append(dst);
        output.write(bytes);
        System.out.println("append to file \t" + conf.get("fs.default.name")
                + fileName);
    }

    /**
     * 列出一个目录下的所有文件或文件夹
     *
     * @param dirName
     * @throws IOException
     */
    public void listFiles(String dirName) throws IOException {
        Path f = new Path(dirName);
        try {
            FileStatus[] status = hdfs.listStatus(f);
            System.out.println(dirName + " has all files:");
            for (int i = 0; i < status.length; i++) {
                System.out.println(status[i].getPath().toString());
            }
        }catch (FileNotFoundException e){
            System.out.println("File "+dirName+" does not exist!");
        }
    }

    /**
     * 查看文件内容
     * @param fileName
     * @throws IOException
     */
    public void catFile(String fileName) throws IOException{
        Path f = new Path(fileName);
        InputStream in = null;
        boolean isExists = hdfs.exists(f);
        if (isExists) {
            try {
                in = hdfs.open(f);
                IOUtils.copyBytes(in, System.out, 1024*1024, false);
            }catch (FileNotFoundException e){
                System.out.println("File "+fileName+" is not a file!");
            } finally{
                IOUtils.closeStream(in);
            }
        }else{
            System.out.println(fileName + "  exist? \t" + isExists);
        }
    }

    /**
     * 删除文件
     *
     * @param fileName
     * @throws IOException
     */
    public void deleteFile(String fileName) throws IOException {
        Path f = new Path(fileName);
        boolean isExists = hdfs.exists(f);
        if (isExists) {
            boolean isDel = hdfs.delete(f, true);
            System.out.println(fileName + "  delete? \t" + isDel);
        } else {
            System.out.println(fileName + "  exist? \t" + isExists);
        }
    }

}
