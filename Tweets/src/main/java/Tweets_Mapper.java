import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.json.*;


public class Tweets_Mapper extends Mapper<Object, Text, Text, Text> {

    public void map(Object key, Text value, Mapper.Context context) throws IOException, InterruptedException {

        String _user_id;
        String _id;
        String _tweet_text;
        String _created_at;
        String _location;

        String line = value.toString();
        String[] tuple = line.split("\\n");
        try {
            for (String s : tuple) {
                JSONObject obj = new JSONObject(s);
                _id = obj.getString("id");
                _user_id = obj.getString("user_id");
                _tweet_text = obj.getString("text");
                _created_at = obj.getString("created_at");
                _location = obj.getString("location");
                context.write(Integer.parseInt(_user_id), Integer.parseInt(_id) + "\t" + _created_at + "\t" + _location + "\t" + _tweet_text);
            }
        } catch (JSONException e) {
            e.printStackTrace();
        }
    }
}