package com.eb.bi.rs.algorithm.pac.Util;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

public class FileUtil {
    public static List<String> getFileLines(File file, String encoding)
            throws IOException {
        List lines = new ArrayList();
        if (encoding == null) {
            encoding = "utf-8";
        }
        FileInputStream stream = new FileInputStream(file);
        InputStreamReader reader = new InputStreamReader(stream, encoding);
        BufferedReader bufferedReader = new BufferedReader(reader);
        String line = null;
        while (((line = bufferedReader.readLine()) != null) && (
                line.trim().length() > 0)) {
            lines.add(line);
        }
        bufferedReader.close();
        reader.close();
        stream.close();
        return lines;
    }

    public static String getFileBody(File file, String encoding) {
        StringBuffer buffer = new StringBuffer();
        try {
            List lines = getFileLines(file, encoding);
            for (Iterator localIterator = lines.iterator(); localIterator.hasNext(); ) {
                String line = (String) localIterator.next();

                buffer.append(line);
                buffer.append("\n");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return buffer.toString();
    }

    public static String getFileBody(String path, String encoding) {
        File file = new File(path);
        if (!(file.exists())) {
            return null;
        }
        return getFileBody(file, encoding);
    }

    public static void mergeFile(String dir, int depth, String outPath, String encoding) {
        File dirFile = new File(dir);
        List filesForMerge = collectFiles(dirFile, depth, -1);
        StringBuffer buffer = new StringBuffer();
        for (Iterator localIterator = filesForMerge.iterator(); localIterator.hasNext(); ) {
            File file = (File) localIterator.next();

            String temp = getFileBody(file, encoding);
            buffer.append(temp);
            buffer.append("\n");
        }
        try {
            PrintWriter pw = new PrintWriter(outPath);
            pw.write(buffer.toString());
            pw.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }

    public static List<File> collectFiles(File dirFile, int depth, int sampleNum) {
        File[] subFiles;
        File file;
        int i;
        int j;
        File[] arrayOfFile1;
        List filesForMerge = new ArrayList();
        if (1 == depth) {
            subFiles = dirFile.listFiles();
            j = (arrayOfFile1 = subFiles).length;
            for (i = 0; i < j; ++i) {
                file = arrayOfFile1[i];

                if (file.isDirectory()) {
                    continue;
                }
                filesForMerge.add(file);
            }
            filesForMerge = sampleFiles(filesForMerge, sampleNum);
        } else {
            subFiles = dirFile.listFiles();
            j = (arrayOfFile1 = subFiles).length;
            for (i = 0; i < j; ++i) {
                file = arrayOfFile1[i];

                if (!(file.isDirectory())) {
                    continue;
                }
                List files = collectFiles(file, depth - 1, sampleNum);
                if (files.size() > 0) {
                    filesForMerge.addAll(files);
                }
            }
        }
        return filesForMerge;
    }

    private static List<File> sampleFiles(List<File> origFiles, int sampleNum) {
        List sampleFiles = new ArrayList();
        int totalSize = origFiles.size();
        if ((sampleNum >= 0) && (totalSize > sampleNum)) {
            if (sampleNum < totalSize / 10) {
                sampleNum = totalSize / 10;
            }
            Random random = new Random();
            int count = 0;
            while (count < sampleNum) {
                int pos = random.nextInt(totalSize);
                File nextFile = (File) origFiles.get(pos);
                if (sampleFiles.contains(nextFile)) {
                    continue;
                }
                sampleFiles.add(nextFile);
                ++count;
            }
            return sampleFiles;
        }
        return origFiles;
    }

    public static String getDifferPath(File subFile, File ancestorFile) {
        File parentFile = subFile.getParentFile();
        String dirpath = "";
        while (!(parentFile.getPath().equalsIgnoreCase(ancestorFile.getPath()))) {
            dirpath = String.format("%s\\%s", new Object[]{parentFile.getName(), dirpath});
            parentFile = parentFile.getParentFile();
        }
        return dirpath;
    }

    public static void makeDir(String path) {
        File dirFile = new File(path);
        if (!(dirFile.exists())) {
            dirFile.mkdirs();
        }
    }

    public static File findDecestorFile(File searchDir, String toFindFileName) {
        if (searchDir.getName().equalsIgnoreCase(toFindFileName)) {
            return searchDir;
        }
        if (searchDir.isDirectory()) {
            File[] arrayOfFile1;
            File[] subFiles = searchDir.listFiles();
            int j = (arrayOfFile1 = subFiles).length;
            for (int i = 0; i < j; ++i) {
                File subFile = arrayOfFile1[i];

                File temp = findDecestorFile(subFile, toFindFileName);
                if (temp != null) {
                    return temp;
                }
            }
        }
        return null;
    }

    public static File findFile(String fileName, File parentFile, int compType) {
        File file;
        int i;
        int j;
        File[] arrayOfFile1;
        File[] files = parentFile.listFiles();
        switch (compType) {
            case 0:
                j = (arrayOfFile1 = files).length;
                for (i = 0; i < j; ++i) {
                    file = arrayOfFile1[i];

                    if (file.getName().equalsIgnoreCase(fileName)) {
                        return file;
                    }
                }
                break;
            case 1:
                j = (arrayOfFile1 = files).length;
                for (i = 0; i < j; ++i) {
                    file = arrayOfFile1[i];

                    if (file.getName().toLowerCase().startsWith(fileName.toLowerCase())) {
                        return file;
                    }
                }
                break;
            case 2:
                j = (arrayOfFile1 = files).length;
                for (i = 0; i < j; ++i) {
                    file = arrayOfFile1[i];

                    if (file.getName().toLowerCase().indexOf(fileName.toLowerCase()) != -1) {
                        return file;
                    }

                }

        }

        return null;
    }
}