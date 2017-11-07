package booklistrec.booklist_sort;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;

/**
 * Created by liyang on 2016/4/28.
 */
public class BookListSortReducer extends Reducer<Text, Text, Text, NullWritable> {
    @Override
    public void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        ArrayList<SheetScore> sheetList = new ArrayList<SheetScore>();
        for (Text val : values) {
            String[] line = val.toString().split("\\|");
            if (line.length >= 2) {
                String sheetId = line[0];
                double score = Double.parseDouble(line[1]);
                SheetScore sheetScore = new SheetScore(sheetId, score);
                sheetList.add(sheetScore);
            }
        }
        Collections.sort(sheetList, new SortByScore());
        StringBuffer result = new StringBuffer();
        if (sheetList.size() > 10) {
            for (int i = 0; i < 10; i++) {
                String sheet = sheetList.get(i).sheetId;
                double score = sheetList.get(i).score;
                result.append(sheet + "|" + score + "|");
            }
            result.deleteCharAt(result.length() - 1);
            context.write(new Text(key.toString() + "|" + result), NullWritable.get());
        } else {
            for (int j = 0; j < sheetList.size(); j++) {
                String sheet = sheetList.get(j).sheetId;
                double score = sheetList.get(j).score;
                result.append(sheet + "|" + score + "|");
            }
            result.deleteCharAt(result.length() - 1);
            context.write(new Text(key.toString() + "|" + result), NullWritable.get());
        }
    }
}
