package booklistrec.booklist_book;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by liyang on 2016/4/25.
 */
public class BookListBookMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        String[] line = value.toString().split("\\|");
        if(line.length >= 6){
            String sheetId = line[0];
            String book = line[3];
            context.write(new Text(sheetId + "|" + book),NullWritable.get());
        }
    }
}
