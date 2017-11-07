package com.eb.bi.rs.mras2.bookrec.guessyoulike;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.*;
import java.util.Map.Entry;

public class SelectBookSimilarityMapper extends Mapper<Object, Text, Text, BookInfo> {
    //1213096|10722911|3302.0|1067.0|19.0|0.0057540885|0.017806936|0.0117805125|0.3231375|2
    //图书A|图书B|图书A用户数|图书B用户数|图书AB共同用户数|前置信度|后置信|KULC|IR|classtype
    //图书A|图书B|来源|相似度
    //新的需求：
    //若type='1'的相似图书不足20本，则用type=‘2’补足20本，type=‘2’的图书KULC全部设置成0.05，尽量保证有书推荐，最多100本
    //选取关联图书的时候，直接筛选出推荐池中的图书，

    private Set<String> recRepo = new HashSet<String>();
    private Map<String, TreeSet<BookInfo>> map = new HashMap<String, TreeSet<BookInfo>>();
    private int selectMaxNumber;

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split("\\|");
        if (fields.length == 10) {
            String srcBookId = fields[0];
            String dstBookId = fields[1];
            if (recRepo.contains(dstBookId)) {    //直接筛选出推荐池中的图书
                BookInfo book = new BookInfo(dstBookId, Double.parseDouble(fields[5]), Short.parseShort(fields[9]));
                if (map.containsKey(srcBookId)) {
                    TreeSet<BookInfo> books = map.get(srcBookId);
                    if (books.size() < selectMaxNumber) {
                        books.add(book);
                    } else if (book.compareTo(books.first()) > 0) {
                        books.remove(books.first());
                        books.add(book);
                    }
                } else {
                    TreeSet<BookInfo> books = new TreeSet<BookInfo>();
                    books.add(book);
                    map.put(srcBookId, books);
                }
            }
        }
    }


    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        selectMaxNumber = conf.getInt("select.max.number", 100);
        URI[] localFiles = context.getCacheFiles();
        for (URI localFile : localFiles) {
            String line;
            BufferedReader in = null;
            try {
                Path path = new Path(localFile.getPath());
                in = new BufferedReader(new FileReader(path.getName().toString()));
                while ((line = in.readLine()) != null) { //图书ID|定制标签|版面集
                    String[] fields = line.split("\\|", -1);
                    if (fields.length == 3) {
                        recRepo.add(fields[0]);
                    }
                }
            } finally {
                if (in != null) {
                    in.close();
                }
            }
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        Iterator<Entry<String, TreeSet<BookInfo>>> iter = map.entrySet().iterator();
        while (iter.hasNext()) {
            Entry<String, TreeSet<BookInfo>> item = iter.next();
            String srcBookId = item.getKey();
            TreeSet<BookInfo> books = item.getValue();
            for (BookInfo book : books) {
                context.write(new Text(srcBookId), book);
            }
        }
    }
}
