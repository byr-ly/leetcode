package booklistrec.booklist_desc;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by liyang on 2016/4/25.
 */
public class BookListDescMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        String[] line = value.toString().split("\\|");
        if (line.length >= 6) {
            String sheetId = line[0];
            String sheetName = line[1];
            String sheetDesc = line[2];
            String type = line[4];
            String tag = line[5];
            StringBuffer result = new StringBuffer();
            result.append(sheetId + "|" + sheetName + "|" + sheetDesc + "|" + type + "|" + tag);
            context.write(new Text(result.toString()), NullWritable.get());
        }
    }
}
