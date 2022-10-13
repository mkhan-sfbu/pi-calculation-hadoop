import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException; 

import java.util.Scanner;
import java.util.StringTokenizer;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
/*
 * @author: Md Shahadat Hossain Khan (19609)
 */

public class Pi {
	private static double rai = 0f; // radius of circle
	private static String inside = "inside";
	private static String outside = "outside";
	
	// Mapper class
	public static class TokenizerMapper
       extends Mapper<Object, Text, Text, IntWritable>{

		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();

		public void map(Object key, Text value, Context context
						) throws IOException, InterruptedException {
			StringTokenizer itr = new StringTokenizer(value.toString());
			while (itr.hasMoreTokens()) {
				word.set(itr.nextToken());
				if (isDartInside(word.toString())){
					// inside
					context.write(new Text(inside), one);
				} else {
					// outside
					context.write(new Text(outside), one);
				}
			}
		}
		/*
		 * check from (x,y) value, if its in circle / radius
		 */
		private boolean isDartInside(String data){
			String strWithComma = data.substring(1, data.length()-1);
			double x = Double.parseDouble(strWithComma.substring(0, strWithComma.indexOf(',')));
			double y = Double.parseDouble(strWithComma.substring(strWithComma.indexOf(',') + 1, strWithComma.length()));
			// sqrt((r-x)^2 + (r-y)^2) < r
			return (Math.sqrt((rai-x)*(rai-x) + (rai-y)*(rai-y)) < rai) ? true : false;
		}
	}
	

	// Reducer class
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

	/*
	 * generate dart randomly
	 */
	private static void generateDart(FileSystem hdfs, String whereToStore, int totPair) throws Exception {
		hdfs.mkdirs(new Path(whereToStore));
        BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(hdfs.create(new Path(whereToStore + "/generated-pairs.txt"), true)));
		double diameter = rai * 2;
		for (int i = 0; i < totPair; i++) {
			// placing the pair in this format example:(4,5)
			int x = (int) (Math.random() * diameter);
			int y = (int) (Math.random() * diameter);
			bw.write("(" + x + "," + y + ") ");
		}
		bw.flush();
		bw.close();
		hdfs.close();
	}

	/*
	 * execute hadoop job inside java program!
	 */
	private static void executeMapReduce(Configuration conf, String inputPath, String outputPath) throws Exception {
		Path inpt = new Path(inputPath);
		Path outpt = new Path(outputPath);
		Job job = Job.getInstance(conf, "Pi");
		job.setJarByClass(Pi.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setReducerClass(IntSumReducer.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.addInputPath(job, inpt);
		FileOutputFormat.setOutputPath(job, outpt);
		job.waitForCompletion(true);
	}

	/*
	 * storing calculated pi value!
	 */
	private static void storeCalculatedPiValue(FileSystem hdfs, String whereToStore, double calculatedValueToStore) throws Exception {
		BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(hdfs.create(new Path(whereToStore + "/calculated-pi-value.txt"), true)));
		bw.write(String.valueOf("Pi = " + calculatedValueToStore));
		bw.flush();
		bw.close();
		hdfs.close();
	}

	/*
	 * read inside and outside count from hadoop result and calculate pi value
	 */
	private static void calculatePiFromHadoopResult(FileSystem hdfs, String outputPath) throws Exception {
		String z, insd = null, outsd = null;
		RemoteIterator<LocatedFileStatus> fileStatusListIterator = hdfs.listFiles(new Path(outputPath), true);
		while(fileStatusListIterator.hasNext()){
			final LocatedFileStatus curEntry = fileStatusListIterator.next();
			// todo
			if (curEntry.isFile()) {
				Path curFile = curEntry.getPath();
				String flName = curFile.getName();
				String flPart = flName.substring(flName.indexOf('.') + 1, flName.length());
				if (flPart.equals("part-r-00000")) {
					// getting our hadoop output
					
					BufferedReader br=new BufferedReader(new InputStreamReader(hdfs.open(curFile)));
					try {
						while ((z = br.readLine()) != null) {
							if (z.contains(inside)) {
								insd = z.replace(inside, "").trim();
							} else if (z.contains(outside)) {
								outsd = z.replace(outside, "").trim();
							}
						}
					} finally {
					  // you should close out the BufferedReader
					  br.close();
					}
				}
			}
		}
		storeCalculatedPiValue(hdfs, outputPath, calculatePi(insd, outsd));
	}

	/*
	 * pi value calculation
	 */
	private static double calculatePi(String insideCount, String outsideCount) {
		double calculatedPiVal = -1;
		if (insideCount == null && outsideCount == null) {
			System.out.println("Inside: NULL, Outside: NULL");
		} else {
			System.out.println("Inside: "+insideCount+", Outside: "+outsideCount);
			double inVal = insideCount != null ? Double.valueOf(insideCount) : 0f;
			double outVal = outsideCount != null ? Double.valueOf(outsideCount) : 0f;
			if((inVal+outVal) > 0) calculatedPiVal = 4 * (inVal / (inVal+outVal));
		}
		System.out.println("Calculated Pi Value: "+calculatedPiVal);
		return calculatedPiVal;
	}

	// controller
	public static void main(String[] args) throws Exception {
		/*
		 * Generate pair and store into “hadoop” data node
		 */
		// user input to generate dart pair
		Scanner sc = new Scanner(System.in);
		System.out.println("Enter a radious:");
   		rai = sc.nextDouble();
		System.out.println("Enter a total index pair of (x,y):");
		int totPair = sc.nextInt();
		sc.close();

		String inpt = args[0] + "/input";
		String otpt = args[0] + "/output";
		Configuration conf = new Configuration();

		generateDart(FileSystem.get(conf), inpt, totPair);

		/*
		 * Let Map and Reduce work to find each dart position (inside/outside)
		 */
		executeMapReduce(conf, inpt, otpt);

		/*
		 * Finally read result from “hadoop” data node and calculate Pi using those data.
		 */
		calculatePiFromHadoopResult(FileSystem.get(conf), otpt);

	}
}


