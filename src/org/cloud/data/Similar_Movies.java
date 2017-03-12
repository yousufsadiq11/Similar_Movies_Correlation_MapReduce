package org.cloud.data;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.math3.stat.correlation.PearsonsCorrelation;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

public class Similar_Movies extends Configured implements Tool {

	private static final Logger LOG = Logger.getLogger(Similar_Movies.class);

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Similar_Movies(), args);
		System.exit(res);
	}

	public int run(String[] args) throws Exception {
		Configuration conf = new Configuration();
		ArrayList<String> argsList = new ArrayList<String>();
		String input="";
		for (int i = 2; i < args.length; i++)
			{//argsList.add(args[i]);
			if(input.equals("")){
				input=args[i];
			}
			else
				input=input+"^^^"+args[i];
			}
		conf.setStrings("argsList",input);
		Job job3 = Job.getInstance(conf, " JOB3 ");
		job3.setJarByClass(this.getClass());
		FileInputFormat.addInputPaths(job3, args[0]);
		FileOutputFormat.setOutputPath(job3, new Path(args[1]));
		job3.setMapperClass(Similar_Movies_Mapper.class);
		//job3.setReducerClass(Similar_Movies_Reducer.class);
	/*	job3.setMapOutputKeyClass(Text.class);
		job3.setMapOutputValueClass(NullWritable.class);*/
		job3.setOutputKeyClass(Text.class);
		job3.setOutputValueClass(NullWritable.class);
		return job3.waitForCompletion(true) ? 0 : 1;
	}

	public static class Similar_Movies_Mapper extends Mapper<LongWritable, Text, Text, NullWritable> {

		private Text word = new Text();

		private static final Pattern WORD_BOUNDARY = Pattern
				.compile("\\s*\\b\\s*");

		public void map(LongWritable offset, Text lineText, Context context)
				throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			String[] args_input=conf.getStrings("argsList");String input="";
		for(int i=0;i<args_input.length;i++){
			if(input.equals(""))
				input=args_input[i];
			else
				input=input+" "+args_input[i];
		}
		
		/*String text = "0123hello9012hello8901hello7890";
		String match = "-m";

		int index = input.indexOf(match);
		while (index >= 0) {  // indexOf returns -1 if no match found
		    System.out.println(index);
		    index = input.indexOf(match, index + 1);
		}*/		System.out.println(input);
		String[] splitted_query=input.split("-m");
		System.out.println(splitted_query[0]);
		System.out.println(splitted_query[1]);
		System.out.println(splitted_query[3]);
			String line = lineText.toString();
			String splitted[] = line.split("\t");
			
		}
	}
/*
	public static class Similar_Movies_Reducer extends
			Reducer<Text, Text, Text, NullWritable> {

		public void reduce(Text word, Iterable<Text> counts, Context context)
				throws IOException, InterruptedException {
			String sum = "";
			for (Text count : counts) {
				if (sum.equals(""))
					sum = sum + count.toString();
				else
					sum = sum + ";;;" + count.toString();
			}
			ArrayList<Double> list1 = new ArrayList<Double>();
			ArrayList<Double> list2 = new ArrayList<Double>();
			if (sum.contains(";;;")) {
				String splitted[] = sum.split(";;;");
				for (int i = 0; i < splitted.length; i++) {
					String further_split[] = splitted[i].split("!!!%@@@!!!");
					list1.add(Double.parseDouble(further_split[0]));
					list2.add(Double.parseDouble(further_split[1]));
				}
				double dotProduct = 0.0, normA = 0.0, normB = 0.0;
				for (int i = 0; i < list1.size(); i++) {
					dotProduct += list1.get(i) * list2.get(i);
					normA += Math.pow(list1.get(i), 2);
					normB += Math.pow(list2.get(i), 2);
				}
				double cosine = dotProduct / (Math.sqrt(normA) * Math.sqrt(normB));

				double list1_array[] = new double[list1.size()];
				double list2_array[]=new double[list2.size()];
				for(int k=0;k<list1.size();k++){list1_array[k]=list1.get(k);
				list2_array[k]=list2.get(k);
				}
				double corr=new PearsonsCorrelation().correlation(list2_array, list1_array);
				double average_correlation = 0.5 * (cosine + corr);
				//context.write(new Text(word), new DoubleWritable(average_correlation));
				
				//------------------
				double sum_of_products = 0,product_of_sums=0,sum_list1 = 0,sum_list2=0,sqaure_sum1=0,square_sum2=0,squares1=0,squares2=0;	
				double count=list1_array.length;
				for(int i=0;i<list1_array.length;i++){
			
			sum_of_products+=(list1_array[i]*list2_array[i]);
			product_of_sums*=(list1_array[i]+list2_array[i]);
			sum_list1+=list1_array[i];
			sum_list2+=list2_array[i];
			squares1+=list1_array[i]*list1_array[i];
			squares2+=list2_array[i]*list2_array[i];
			
				}
				sqaure_sum1=sum_list1*sum_list1;//sum^2
				square_sum2=sum_list2*sum_list2;
				double numerator=count*sum_of_products-(product_of_sums);
				double denominator=Math.sqrt((count*(sqaure_sum1)-((squares1)*(squares1)))*(((count*square_sum2)-((squares2)*(squares2)))));
				double result=numerator/denominator;
				
				double avg_correlation = 0.5 * (cosine + result);
				context.write(new Text(word), new DoubleWritable(
						avg_correlation));
				
			

			} else {
				String splitted[] = sum.split("!!!%@@@!!!");System.out.println("splittttt"+splitted[1]);
				list1.add(Double.parseDouble(splitted[0]));
				list2.add(Double.parseDouble(splitted[1]));
				System.out.println("else loop" + list1.get(0));
				double dotProduct = 0.0, normA = 0.0, normB = 0.0;
			
					dotProduct += list1.get(0) * list2.get(0);
					normA += Math.pow(list1.get(0), 2);
					normB += Math.pow(list2.get(0), 2);
				
				double cosine = dotProduct / (Math.sqrt(normA) * Math.sqrt(normB));
				double list1_array[] = new double[2];list1_array[0]=list1.get(0);
				double list2_array[]=new double[2];list2_array[0]=list2.get(0);
				double corr=new PearsonsCorrelation().correlation(list2_array, list1_array);
				double average_correlation = 0.5 * (cosine + corr);
							//context.write(new Text(word), new DoubleWritable(	average_correlation));
			
				double sum_of_products = 0,product_of_sums=0,sum_list1 = 0,sum_list2=0,sqaure_sum1=0,square_sum2=0,squares1=0,squares2=0;	
				double count=list1_array.length;
				
			
			sum_of_products+=(list1_array[0]*list2_array[0]);
			product_of_sums*=(list1_array[0]+list2_array[0]);
			sum_list1+=list1_array[0];
			sum_list2+=list2_array[0];
			squares1+=list1_array[0]*list1_array[0];
			squares2+=list2_array[0]*list2_array[0];
			
				
				sqaure_sum1=Math.sqrt(sum_list1);//sum^2
				square_sum2=Math.sqrt(sum_list2);
				double numerator=sum_of_products-(product_of_sums/count);
				double denominator=Math.sqrt(((sqaure_sum1)-((squares1/count)*(squares1/count)))*((square_sum2)-((squares2/count)*(squares2/count))));
				double result=numerator/denominator;
				
				double avg_correlation = 0.5 * (cosine + result);
				context.write(new Text(word), new DoubleWritable(
						avg_correlation));
			}
						
		}
	}*/
}
