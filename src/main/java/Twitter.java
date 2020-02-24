import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.json.JSONArray;
import org.json.JSONObject;

public class Twitter {

    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, IntWritable>{

        private final static IntWritable one = new IntWritable(1);
        private final static DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");

        private final Text hashtagDatePair = new Text();

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString(), "\n");
            while (itr.hasMoreTokens()) {
                final JSONObject json = new JSONObject(itr.nextToken());
                final JSONArray hashtags = json.getJSONObject("entities").getJSONArray("hashtags");
                final String timestamp = json.getString("timestamp_ms");
                final Date date = new Date(Integer.valueOf(timestamp, 10));
                final String dateStr = dateFormat.format(date);

                for(int i = 0;i<hashtags.length();i++) {
                    final JSONObject hashtag = hashtags.getJSONObject(i);
                    final String hashtagStr = hashtag.getString("text").toLowerCase();
                    hashtagDatePair.set(dateStr + "|" + hashtagStr);
                    context.write(hashtagDatePair, one);
                }
            }
        }
    }

    public static class IntSumReducer
            extends Reducer<Text,IntWritable,Text,IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }
    // "C:\Program Files (x86)\hadoop-2.10.0\bin\hadoop.bat" jar Twitter-1.0-SNAPSHOT.jar tweets.json erg.txt
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "word count");
        job.setJarByClass(Twitter.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}