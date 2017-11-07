package booklistrec.booklist_choose;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by liyang on 2016/4/27.
 */
public class BookListChooseMapper extends Mapper<LongWritable, Text, Text, Text> {
    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        String[] line = value.toString().split("\\|");
        if(line.length >= 3){
            String user = line[0];
            context.write(new Text(user),new Text(line[1] + "|" + line[2]));
        }
    }
}
