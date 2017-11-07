package com.eb.bi.rs.mras.authorrec.itemcf.filler;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.eb.bi.rs.mras.authorrec.itemcf.util.JobExecuUtil;
import com.eb.bi.rs.mras.authorrec.itemcf.util.PluginUtil;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class FillerMapper extends Mapper<Object, Text, Text, Text> {
    class FamousAuthorBooks {
        String authorId;
        List<String> books;
    }

    private Map<String, List<FamousAuthorBooks>> famousAuthorMap
            = new HashMap<String, List<FamousAuthorBooks>>();
    private Map<String, Set<String>> initUserReadMap
            = new HashMap<String, Set<String>>();
    private List<FamousAuthorBooks> allAuthorBooks
            = new ArrayList<FamousAuthorBooks>();

    /**
     * @param value: 输入：无预测关联作者的用户
     *               格式：msisdn|第n偏好|偏好classid
     *               map out:
     *               key:msisdn, value:第n偏好|authorid|bookid
     */
    public void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {
        String[] strs = value.toString().split("\\|");
        String msisdn = strs[0];
        int prefIndex = Integer.parseInt(strs[1]);
        int maxSize = 10;
        switch (prefIndex) {
            case 2:
                maxSize = 20;
                break;
            case 3:
                maxSize = 30;
                break;
            case -1:
                maxSize = 40;
                break;
            default:
                break;
        }
        int count = 0;
        String classid = null;
        if (strs.length > 2) {
            classid = strs[2];
        }
        List<FamousAuthorBooks> authorBooks = allAuthorBooks;
        if (prefIndex > 0) {
            authorBooks = famousAuthorMap.get(classid);
        }
        if (authorBooks == null || authorBooks.size() == 0) {
            return;
        }
        Set<String> readBooks = initUserReadMap.get(msisdn);
        int[] authorPoses = getRandomPos(authorBooks.size());
        for (int i = 0; i < authorPoses.length; i++) {
            FamousAuthorBooks authorBook = authorBooks.get(authorPoses[i]);
            String authorid = authorBook.authorId;
            List<String> books = authorBook.books;
            if (books == null || books.size() == 0) {
                continue;
            }
            int[] bookPoses = getRandomPos(books.size());
            for (int j = 0; j < bookPoses.length; j++) {
                String bookid = books.get(bookPoses[j]);
                if (readBooks == null || !readBooks.contains(bookid)) {
                    String temp = String.format("%d|%s|%s",
                            prefIndex, authorid, bookid);
                    context.write(new Text(msisdn), new Text(temp));
                    count++;
                    break;
                }
            }
            if (count == maxSize) {
                break;
            }
        }
    }

    public void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        JobExecuUtil execuUtil = new JobExecuUtil();
        URI[] localFiles = execuUtil.getCacheFiles(context);
        String recommFamousBookPath = context.getConfiguration()
                .get(PluginUtil.TEMP_AUTHOR_FAMOUS_BOOK_KEY);
        recommFamousBookPath = execuUtil.getFileName(recommFamousBookPath);
        String userRead6cmPath = context.getConfiguration()
                .get(PluginUtil.TEMP_DEEP_READ_FILTER_FILLER_KEY);
        userRead6cmPath = execuUtil.getFileName(userRead6cmPath);
        //格式：格式：bookid|authorid|classid|contentstatus|author_grade|typeid
        for (URI path : localFiles) {
            if (!path.toString().contains(recommFamousBookPath)) {
                continue;
            }
            BufferedReader br = null;
            try {
                FileSystem fs = FileSystem.get(context.getConfiguration());
                br = new BufferedReader(new InputStreamReader(fs.open(new Path(path))));
                String line;
                while ((line = br.readLine()) != null) {
                    String[] fields = line.split("\\|");
                    String bookid = fields[0];
                    String authorid = fields[1];
                    String classid = fields[2];
                    addAuthorBook(classid, authorid, bookid);
                    //			randomAuthors.add(authorid);
                }
            } finally {
                if (br != null) {
                    br.close();
                }
            }
        }
        System.out.println(userRead6cmPath);
        //用户待推荐作家图书表DMN.IRECM_US_BKID_6CM
        //格式：msisdn|bookid
        for (URI path : localFiles) {
            if (!path.toString().contains(userRead6cmPath)) {
                continue;
            }
            System.out.println(path.toString());
            BufferedReader br = null;
            try {
                FileSystem fs = FileSystem.get(context.getConfiguration());
                br = new BufferedReader(new InputStreamReader(fs.open(new Path(path))));
                String line;
                while ((line = br.readLine()) != null) {
                    String[] fields = line.split("\\|");
                    String msisdn = fields[0];
                    String bookid = fields[1];
                    Set<String> books = initUserReadMap.get(msisdn);
                    if (books == null) {
                        books = new HashSet<String>();
                    }
                    books.add(bookid);
                    initUserReadMap.put(msisdn, books);
                    //	String temp = String.format("%s|%s", msisdn, bookid);
                    //	initUserReadMap.add(temp);
                }
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                if (br != null) {
                    br.close();
                }
            }
        }
        String logStr = String.format("FillerMapper set up initUserReadMap "
                + "initUserReadMap size: %d", initUserReadMap.size());
        System.out.println(logStr);
    }

    private void addAuthorBook(String classid, String author, String book) {
        List<FamousAuthorBooks> classAuthors = famousAuthorMap.get(classid);
        if (classAuthors == null) {
            classAuthors = new ArrayList<FamousAuthorBooks>();
        }
        addAuthorToList(author, book, classAuthors);
        famousAuthorMap.put(classid, classAuthors);
        addAuthorToList(author, book, allAuthorBooks);
    }

    private void addAuthorToList(String author, String book,
                                 List<FamousAuthorBooks> authorList) {
        boolean bFoundAuthor = false;
        for (FamousAuthorBooks abs : authorList) {
            if (abs.authorId.equals(author)) {
                bFoundAuthor = true;
                abs.books.add(book);
                break;
            }
        }
        if (!bFoundAuthor) {
            FamousAuthorBooks abs = new FamousAuthorBooks();
            abs.authorId = author;
            abs.books = new ArrayList<String>();
            abs.books.add(book);
            authorList.add(abs);
        }
    }

    private static int[] getRandomPos(int size) {
        int[] result = new int[size];
        for (int i = 0; i < size; i++) {
            result[i] = i;
        }
        int endPos = result.length;
        for (int i = 0; i < size; i++) {
            int random = (int) (Math.random() * endPos);
            int val = result[random];
            int lastPosVal = result[endPos - 1];
            result[endPos - 1] = val;
            result[random] = lastPosVal;
            endPos--;
            if (endPos <= 1) {
                break;
            }
        }
        return result;
    }

}
