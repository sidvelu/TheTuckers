package org.apache.hadoop.yelp;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.json.*;

import javax.security.auth.callback.TextInputCallback;

public class CombineBooks1 {

	public static class Map extends Mapper<LongWritable, Text, Text, Text> {

		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {

			String businessId;
			String state;
			String cat;
			JSONArray category;
			String line = value.toString();
			String[] tuple = line.split("\\n");
			try {
				for (int i = 0; i < tuple.length; i++) {
					JSONObject obj = new JSONObject(tuple[i]);
					businessId = obj.getString("business_id");
					state = obj.getString("state");
					category = obj.getJSONArray("categories");
					cat = obj.getJSONArray("categories").toString();
                    boolean restaurant = false;

                    for (int j = 0; j < cat.length(); j++) {
                        if (cat.get(j).toString().equals("Restaurant")) {
                            restaurant = true;
                        }
                    }

                    if (restaurant == true) {
					    context.write(new Text(state), new Text(cat));
                    }
				}
			} catch (JSONException e) {
				e.printStackTrace();
			}
		}

	}

	public static class Reduce extends Reducer<Text, Text, NullWritable, Text> {

		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			HashMap<String, Integer> map = new HashMap<String, Integer>();
			try {

				JSONObject obj = new JSONObject();
				JSONArray ja = new JSONArray();
				int total_items = 0;
				for (Text val : values) {

					//JSONObject obj1 = new JSONObject(val);

					String x = val.toString();
					// JSONArray categories = obj.getJSONArray("category");

					x = x.replace("\"", "");
					x = x.replace("[", "");
					x = x.replace("]", "");
					String[] categories = x.split(",");

					for (int m = 0; m < categories.length; m++) {
						total_items++;
						if (map.containsKey(categories[m])) {

							int count = map.get(categories[m]);

							map.put(categories[m], count + 1);

						} else {
							map.put(categories[m], 1);
						}

					}
				}
				
				int length = map.size();
				
				for (int i = 0; i < length; i++) {
					int maxVal = -1;
					String maxKey = "";
					for (String str: map.keySet()){
						int count = map.get(str);
						if (count > maxVal){
							maxKey = str;
							maxVal = count;			
						}
					}
					
					Double percent = ((double) maxVal / (double) total_items) * 100;
					String output = key.toString() + " " + maxKey + " " + percent.toString() + " " + maxVal;
					context.write(NullWritable.get(), new Text(output));
					
					map.remove(maxKey);
				}
					
				
				
				// ja.put(jo);
				/*
				for (String str : map.keySet()) {
					int count = map.get(str);
					Double percent = ((double) count / (double) total_items) * 100;
					String output = key.toString() + " " + str + " " + percent.toString() + " " + count;
					context.write(NullWritable.get(), new Text(output));
				}*/

			} catch (JSONException e) {
				e.printStackTrace();
			}
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		if (args.length != 2) {
			System.err.println("Usage: CombineBooks <in> <out>");
			System.exit(2);
		}

		Job job = new Job(conf, "CombineBooks");
		job.setJarByClass(CombineBooks1.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		// job.setCombinerClass(Reducer.class);
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
}
