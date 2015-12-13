import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;


public class YelpDatasetChallenge {
	public static class MapNeighborhoods extends Mapper<LongWritable, Text, Text, MapWritable>{
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{

			String line = value.toString();
			String[] tuple = line.split("\\n");
			try{
				for(int i=0;i<tuple.length; i++){
					JSONObject obj = new JSONObject(tuple[i]);

					// Extracting neighborhoods array
					JSONArray neighborhoods = obj.getJSONArray("neighborhoods");

                    // Extracting rating and review count
					double rating = Double.parseDouble(obj.getString("stars"));
					int reviewCount = Integer.parseInt(obj.getString("review_count")); 

                    // Get the name of the restaurant
					String name = obj.getString("name");
					Double ratingValue = rating * (double) reviewCount;
					
                    // Gets the categories
					JSONArray categories = obj.getJSONArray("categories");
					
                    // Combines categories into a  string so they can be put in
                    // a StringWritable object
					String cats = "";
					for(int j = 0; j < categories.length(); j++){
						cats = cats + categories.get(i).toString() + "~";
					}
					
                    // Iterates though each neighborhood
					for(int j = 0; j < neighborhoods.length(); j++) {
						String neighborhood = neighborhoods.get(i).toString();
						MapWritable mapValue = new MapWritable();

                        // Only if rating is above a certain value will the restaurant be passed 
                        // to the reducer
						if (rating >= 4.0){
							Text mapKey = new Text(neighborhood + "~" + ratingValue.toString());
							mapValue.put(new IntWritable(0), new Text(name));
							mapValue.put(new IntWritable(1), new Text(cats));
							context.write(mapKey, mapValue);
						}
					}

				}
			}catch(JSONException e){
				e.printStackTrace();
			}
		}
	}

    /* Reduce task that writes the top restaurants in each neighborhood and classifies it */
	public static class NeighborhoodReducer extends Reducer<Text,MapWritable,Text,Text> {
		private IntWritable result = new IntWritable();
		public void reduce(Text key, Iterable<MapWritable> values, Context context) throws IOException, InterruptedException {
			int count_max = 5;
			int count = 0;
			
			String nhood = "";
			
            // Maps all the categoies into a hashmap
			HashMap<String, Integer> hmap = new HashMap<String, Integer>();

            // For each restaurant
			for (MapWritable val : values) {
				if (count++ >= count_max) {
					break;
				}

				String name = val.get(new IntWritable(0)).toString();
				String[] catt = val.get(new IntWritable(1)).toString().split("~");
				//int reviewCount = ((IntWritable) val.get(new IntWritable(1))).get();
				System.out.println(count + " " + name);

				//String x = rating + " " + reviewCount;
				String[] arr = key.toString().split("~");
				String x = arr[1] + " " + name;
                // Write the restaurant to the text output
				context.write(new Text(arr[0]), new Text(x));
				nhood = arr[0];
				
				for (String cat : catt) {
					if (hmap.containsValue(cat)) {
						hmap.put(cat, hmap.get(cat)+ 1);
					} else {
						hmap.put(cat, (Integer) 1);
					}
				}

			}
			
			
            // Finds the max category
			String maxcat= "";
			int max = -1;
			for(String category: hmap.keySet()){
				if (hmap.get(category) > max) {
					maxcat = category;
					max = hmap.get(category);
				}
			}
			context.write(new Text("CLASSIFICAITION: " + nhood), new Text(maxcat));
			
		}
	}

	public static class PlanePartitioner
	extends Partitioner<Text, DoubleWritable> {

		// Make sure each airline goes to the same reducer

		@Override
		public int getPartition(Text key, DoubleWritable value, int numTasks) {
			return key.toString().split("~")[0].hashCode() % numTasks;
		}
	}

	public static class KeyComparator extends WritableComparator {
		protected KeyComparator() {
			super(Text.class, true);
		}

		// KeyComparator for secondary sorting (a bit complicated because
		// I didn't use the ID which is an int instead of a string)

		public int compare(WritableComparable w1, WritableComparable w2) {
			Text ip1 = (Text) w1; Text ip2 = (Text) w2;
			String ip1First, ip2First;
			Double ip1Second, ip2Second;
			String ip1Split[] = ip1.toString().split("~");
			String ip2Split[] = ip2.toString().split("~");
			ip1First = ip1Split[0];
			ip1Second = Double.parseDouble(ip1Split[1]);
			ip2First = ip2Split[0];
			ip2Second = Double.parseDouble(ip2Split[1]);

			int cmp = ip1First.compareTo(ip2First);
			if (cmp != 0) {
				return cmp;
			}
			return -ip1Second.compareTo(ip2Second);
		}
	}

	public static class GroupComparator extends WritableComparator {
		protected GroupComparator() {
			super(Text.class, true);
		}
		// Group Comparator for secondary sorting (also more complex
		// than usual because I used the string instead of int to identify
		// carriers)

		public int compare(WritableComparable w1, WritableComparable w2) {
			Text ip1 = (Text) w1; Text ip2 = (Text) w2;
			String ip1First, ip2First;
			Double ip1Second, ip2Second;
			String ip1Split[] = ip1.toString().split("~");
			String ip2Split[] = ip2.toString().split("~");
			ip1First = ip1Split[0];
			ip1Second = Double.parseDouble(ip1Split[1]);
			ip2First = ip2Split[0];
			ip2Second = Double.parseDouble(ip2Split[1]);
			return ip1First.compareTo(ip2First);
			/*
                int cmp = ip1First.compareTo(ip2First);
                if (cmp != 0) {
                        return cmp;
                }
                return ip1Second.compareTo(ip2Second);*/
		}
	}




	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "top nieghborhood");
		job.setJarByClass(YelpDatasetChallenge.class);
		job.setMapperClass(MapNeighborhoods.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(MapWritable.class);
		//job.setCombinerClass(NeighborhoodReducer.class);
		job.setReducerClass(NeighborhoodReducer.class);
		job.setPartitionerClass(PlanePartitioner.class);
		job.setSortComparatorClass(KeyComparator.class);
		job.setGroupingComparatorClass(GroupComparator.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setNumReduceTasks(1);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
