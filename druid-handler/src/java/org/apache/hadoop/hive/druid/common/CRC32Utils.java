package org.apache.hadoop.hive.druid.common;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.zip.CRC32;

/**
 *
 */
public class CRC32Utils
{
    public static String CRC_NAME = "._crc";

    public static String getCRC32(File dir)
    {
        if (dir == null || !dir.exists() || !dir.isDirectory()) {
            return "";
        }
        File[] filelist = dir.listFiles();
        if (filelist == null || filelist.length == 0) {
            return "";
        }

        CRC32 crc32 = new CRC32();
        crc32.update(0);
        long filesize=0;
        StringBuffer buff=new StringBuffer();
        if(filelist!=null) {
            buff.append(filelist.length).append("_");
            for(File file:filelist) {
                if (CRC_NAME.equals(file.getName())) {
                    continue;
                }
                crc32.update(file.getName().getBytes());
                filesize+=file.length();
            }
        }
        long crcvalue = crc32.getValue();
        buff.append(crcvalue).append("_");
        buff.append(filesize);
        return buff.toString();
    }

    public static boolean verify(File dir)
    {
        if (dir == null || !dir.exists()) {
            return false;
        }
        File crc32File = new File(dir, CRC_NAME);
        //means no verify
        if (!crc32File.exists()) {
            return true;
        }
        try {
            String crc32old = new String(Files.readAllBytes(crc32File.toPath()));
            String crc32Now = getCRC32(dir);
            if (crc32Now.equals(crc32old)) {
                return true;
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return false;
    }

    public static long getFilesLength(String crc32)
    {
        return Long.parseLong(crc32.split("_", -1)[2]);
    }

    public static long getFilesLength(File dir)
    {
        if (dir == null || !dir.exists()) {
            return 0;
        }
        File crc32File = new File(dir, CRC_NAME);
        if (crc32File.exists()) {
            try {
                String crc32 = new String(Files.readAllBytes(crc32File.toPath()));
                return getFilesLength(crc32);
            } catch (IOException e) {}
        }
        File[] filelist = dir.listFiles();
        long filesize=0;
        if (filelist != null) {

            for(File file:filelist) {
                if (CRC_NAME.equals(file.getName())) {
                    continue;
                }
                filesize+=file.length();
            }
        }
        return filesize;
    }


    public static void main(String[] args) {
        System.out.println(Integer.MAX_VALUE);
    }
}
