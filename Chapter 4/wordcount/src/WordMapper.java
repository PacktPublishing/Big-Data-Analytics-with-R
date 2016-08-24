import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.util.regex.*;

public class WordMapper extends Mapper<Object, Text, Text, IntWritable> {
    private final IntWritable one = new IntWritable(1);
    private Text word = new Text();
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] words = value.toString().replaceAll("\\p{Punct}", "").toLowerCase().split(" ");
        
        for (String str: words)
        {
            word.set(str);
            context.write(word, one);
        }
    }
}
