import java.io.IOException;
import java.util.*;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class dist {
	
	public static class TokenizerMapper extends Mapper<Object, Text, Text, Text> {
		private Text one = new Text();
		private Text word = new Text();

	   public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		   
		   String fileName = ((org.apache.hadoop.mapreduce.lib.input.FileSplit) context.getInputSplit()).getPath().getName();
		   String paragraph = value.toString().toLowerCase().replaceAll("[^a-zA-Z0-9]", " ");
		   StringTokenizer itr = new StringTokenizer(paragraph);
		   
		   while(itr.hasMoreTokens()) {
			   String currentWord = itr.nextToken();
			   
			   if(currentWord.startsWith("ex")) {
				   word.set(fileName);
				   one.set(currentWord);
				   context.write(word, one);
			   }
		   }
	   }
	   
	}
	
	public static class FileReducer extends Reducer<Text,Text,Text,Text> {
		
		private Map<String, Integer> mTotal = new TreeMap<String, Integer>();
		
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			
			Map<String, Integer> m = new TreeMap<String, Integer>();
			
			for(Text value: values) {
				String word = value.toString();
				
				if(m.getOrDefault(word, 0) != Integer.MAX_VALUE)
					m.put(word, m.getOrDefault(word, 0) + 1);
				
				if(mTotal.getOrDefault(word, 0) != Integer.MAX_VALUE)
					mTotal.put(word, mTotal.getOrDefault(word, 0) + 1);
			}
			
			context.write(key, new Text(print(m)));
		}
		
		protected void cleanup(Context context) throws IOException, InterruptedException {
			context.write(new Text("Total"), new Text(print(mTotal)));
		}
		
		private String print(Map<String, Integer> m) {
			List<Map.Entry<String, Integer>> list = new ArrayList<Entry<String, Integer>>(m.entrySet());
			
			Collections.sort(list, new Comparator<Map.Entry<String, Integer>>() {
				
				public int compare(Map.Entry<String, Integer> object1, Map.Entry<String, Integer> object2) {
					
					if(object1.getValue().equals(object2.getValue()))
						return object1.getKey().compareTo(object2.getKey());
					
					else
						return object2.getValue().compareTo(object1.getValue());
				}
			});
			
			StringBuilder str = new StringBuilder();
			
			for(int i = 0; i < list.size(); i++) {
				str.append(list.get(i).getKey() + ", " + list.get(i).getValue());
				if(i < list.size() - 1)
					str.append(", ");
			}
			
			return str.toString();
		}
		
	}
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "dist");
		job.setJarByClass(dist.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setReducerClass(FileReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setNumReduceTasks(1);
		FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
		
	}
	
}
