import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Mapper;
import org.json.*;

public class Tweets_Mapper {

    public void map(LongWritable key, Text value, Mapper.Context context) throws IOException, InterruptedException{

        String _user_id;
        String _id;
        String _tweet_text;
        String _created_at;
        String _location;

        String line = value.toString();
        String[] tuple = line.split("\\n");
        try{
            for (String s : tuple) {
                JSONObject obj = new JSONObject(s);
                _id = obj.getString("id");
                _user_id = obj.getString("user_id");
                _tweet_text = obj.getString("text");
                _created_at = obj.getString("created_at");
                _location = obj.getString("location");
                context.write(Integer.parseInt(_user_id), Integer.parseInt(_id) + "\t" + _created_at + "\t" + _location + "\t" + _tweet_text);
            }
        }catch(JSONException e){
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        if (args.length != 2) {
            System.err.println("Usage: CombineBooks <in> <out>");
            System.exit(2);
        }

        Job job = new Job(conf, "CombineBooks");
        job.setJarByClass(CombineBooks.class);
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);



}
