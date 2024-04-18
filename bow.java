import java.io.IOException;
import java.util.StringTokenizer;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class bow {
	
	private final static String[] top100Word = { "the", "be", "to", "of", "and", "a",
			"in", "that", "have", "i", "it", "for", "not", "on", "with", "he", "as", "you", "do",
			"at", "this", "but", "his", "by", "from", "they", "we", "say", "her", "she", "or", "an",
			"will", "my", "one", "all", "would", "there", "their", "what", "so", "up", "out", "if",
			"about", "who", "get", "which", "go", "me", "when", "make", "can", "like", "time",
			"no", "just", "him", "know", "take", "people", "into", "year", "your", "good",
			"some", "could", "them", "see", "other", "than", "then", "now", "look", "only",
			"come", "its", "over", "think", "also", "back", "after", "use", "two", "how", "our",
			"work", "first", "well", "way", "even", "new", "want", "because", "any", "these",
			"give", "day", "most", "us" };
	
	public static class TokenizerMapper extends Mapper<Object, Text, Text, Text> {

	
		private final Text one = new Text();
		private final Text word = new Text();
		private final static Set<String> wset = new HashSet<String>(Arrays.asList(top100Word));

	   public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		   
		   String fileName = ((org.apache.hadoop.mapreduce.lib.input.FileSplit) context.getInputSplit()).getPath().getName();
		   String paragraph = value.toString().toLowerCase().replaceAll("[^a-zA-Z0-9]", " ");
		   StringTokenizer itr = new StringTokenizer(paragraph);
		   
		   while(itr.hasMoreTokens()) {
			   String currentWord = itr.nextToken();
			   
			   if(wset.contains(currentWord)) {
				   word.set(fileName);
				   one.set(currentWord);
				   context.write(word, one);
			   }
		   }
	   }
	}
	
	public static class ArrayReducer extends Reducer<Text,Text,Text,Text> {
		
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			int arr[] = new int[top100Word.length];
			
			for(Text value: values) {
				String word = value.toString();
				
				for(int i = 0; i < top100Word.length; i++) {
					if(top100Word[i].equals(word) && arr[i] != Integer.MAX_VALUE)
						arr[i] += 1;
				}
			}
			
			context.write(key, new Text(print(arr)));
		}
		
		private String print(int[] arr) {
			StringBuilder str = new StringBuilder();
			
			for(int i = 0; i < arr.length; i++) {
				str.append(arr[i]);
				
				if(i < arr.length - 1)
					str.append(", ");
			}
			
			return str.toString();
		}
		
	}
	
	public static void main(String[] args) throws Exception {
		
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "bow");
		job.setJarByClass(bow.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setReducerClass(ArrayReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setNumReduceTasks(1);
		FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
		
	}
	
}