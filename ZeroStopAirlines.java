package AirlineAnalysis;



	import java.io.IOException;
	import java.util.StringTokenizer;

	import org.apache.hadoop.conf.Configuration;
	import org.apache.hadoop.fs.Path;
	import org.apache.hadoop.io.IntWritable;
	import org.apache.hadoop.io.LongWritable;
	import org.apache.hadoop.io.Text;
	import org.apache.hadoop.mapreduce.Job;
	import org.apache.hadoop.mapreduce.Mapper;
	import org.apache.hadoop.mapreduce.Reducer;
	import org.apache.hadoop.mapreduce.Reducer.Context;
	import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
	import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
	import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
	import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

	

	public class ZeroStopAirlines {

		
		public static class Map extends Mapper<LongWritable,Text,Text,IntWritable>{

			public void map(LongWritable key, Text value,
					Context context)
					throws IOException,InterruptedException {
				
				String str=value.toString();
				String zsAirline[]=str.split(",");
				if(zsAirline[7].equals("0")){
				value.set(zsAirline[1]);
				context.write(value,new IntWritable(1));
				}
			}
			
		}
		
				
		public static void main(String[] args) throws Exception {
			// TODO Auto-generated method stub
			
			//JobConf conf = new JobConf(WordCount.class);
			Configuration conf= new Configuration();
			
			
			//conf.setJobName("mywc");
			Job job = new Job(conf,"mywc");
			
			job.setJarByClass(ZeroStopAirlines.class);
			job.setMapperClass(Map.class);
			//job.setReducerClass(Reduce.class);
			
			job.setNumReduceTasks(1);

			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(IntWritable.class);
			
			job.setInputFormatClass(TextInputFormat.class);
			job.setOutputFormatClass(TextOutputFormat.class);
			
			

			Path outputPath = new Path(args[1]);
				
		        //Configuring the input/output path from the filesystem into the job
		        
		    FileInputFormat.addInputPath(job, new Path(args[0]));
		    FileOutputFormat.setOutputPath(job, new Path(args[1]));
				
				//deleting the output path automatically from hdfs so that we don't have delete it explicitly
				
			outputPath.getFileSystem(conf).delete(outputPath);
				
				//exiting the job only if the flag value becomes false
				
			System.exit(job.waitForCompletion(true) ? 0 : 1);
		}
	}



