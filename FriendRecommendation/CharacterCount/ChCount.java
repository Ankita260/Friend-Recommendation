import java.io.IOException; 
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration; 
import org.apache.hadoop.fs.FileSystem; 
import org.apache.hadoop.fs.Path; 
import org.apache.hadoop.io.IntWritable; 
import org.apache.hadoop.io.LongWritable; 
import org.apache.hadoop.io.Text; 
import org.apache.hadoop.mapreduce.Job; 
import org.apache.hadoop.mapreduce.Mapper; 
import org.apache.hadoop.mapreduce.Reducer; 
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat; 
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class ChCount {

public static class WordCountMapper 
extends Mapper < LongWritable, Text, Text, IntWritable >
{

private final static IntWritable one = new IntWritable( 1); 
private Text word = new Text();

@Override 
public void map( LongWritable key, Text value, Context context
) throws IOException, 
InterruptedException { 
StringTokenizer s = new 
StringTokenizer( value.toString()); 

while (s.hasMoreTokens()) { 
	String token = s.nextToken();
	if(token.startsWith("e")){
		char token1 = token.charAt(0);
word.set("e:"); 
context.write( word, one); 
}
	if(token.startsWith("m")){
		char token1 = token.charAt(0);
word.set("m:"); 
context.write( word, one); 
}
	if(token.startsWith("l")){
		char token1 = token.charAt(0);
word.set("l:"); 
context.write( word, one); 
}
	if(token.startsWith("q")){
		char token1 = token.charAt(0);
word.set("q:"); 
context.write( word, one); 
} 
	if(token.startsWith("h")){
		char token1 = token.charAt(0);
word.set("h:"); 
context.write( word, one); 
} 
	if(token.startsWith("f")){
		char token1 = token.charAt(0);
word.set("f:"); 
context.write( word, one); 
	}
	if(token.startsWith("o")){
		char token1 = token.charAt(0);
word.set("o:"); 
context.write( word, one); 
	}
	if(token.startsWith("k")){
		char token1 = token.charAt(0);
word.set("k:"); 
context.write( word, one); 
}
	if(token.startsWith("i")){
		char token1 = token.charAt(0);
word.set("i:"); 
context.write( word, one); 
	}
	if(token.startsWith("c")){
		char token1 = token.charAt(0);
word.set("c:"); 
context.write( word, one);
	}
	if(token.startsWith("a")){
		char token1 = token.charAt(0);
word.set("a:"); 
context.write( word, one); 
	}
	if(token.startsWith("n")){
		char token1 = token.charAt(0);
word.set("n:"); 
context.write( word, one); 
	}
	if(token.startsWith("d")){
		char token1 = token.charAt(0);
word.set("d:"); 
context.write( word, one); 
	}
	if(token.startsWith("x")){
		char token1 = token.charAt(0);
word.set("x:"); 
context.write( word, one);
	}
	if(token.startsWith("b")){
		char token1 = token.charAt(0);
word.set("b:"); 
context.write( word, one);
	}
	if(token.startsWith("i")){
		char token1 = token.charAt(0);
word.set("i:"); 
context.write( word, one); 
	}
	if(token.startsWith("t")){
		char token1 = token.charAt(0);
word.set("t:"); 
context.write( word, one); 
	}
	if(token.startsWith("x")){
		char token1 = token.charAt(0);
word.set("x:"); 
context.write( word, one); 
	}
	if(token.startsWith("y")){
		char token1 = token.charAt(0);
word.set("y:"); 
context.write( word, one);
	}
	if(token.startsWith("s")){
		char token1 = token.charAt(0);
word.set("t:"); 
context.write( word, one); 
	}
	if(token.startsWith("u")){
		char token1 = token.charAt(0);
word.set("u:"); 
context.write( word, one); 
	}
	if(token.startsWith("r")){
		char token1 = token.charAt(0);
word.set("r:"); 
context.write( word, one); 
	}
	if(token.startsWith("w")){
		char token1 = token.charAt(0);
word.set("w:"); 
context.write( word, one); 
	}
	if(token.startsWith("z")){
		char token1 = token.charAt(0);
word.set("z:"); 
context.write( word, one);
	}
	if(token.startsWith("w")){
		char token1 = token.charAt(0);
word.set("w:"); 
context.write( word, one); 
	}
	if(token.startsWith("z")){
		char token1 = token.charAt(0);
word.set("z:"); 
context.write( word, one); 
	}
}
}
}
public static class WordCountReducer 
extends 
Reducer < Text, IntWritable, Text, IntWritable > { 
private IntWritable result = new IntWritable(); 
@Override 
public void reduce( Text key, Iterable < IntWritable > values, Context context 
) throws IOException, 
InterruptedException { 
int sum = 0; 
for (IntWritable val : values) { 
sum += val.get(); 
} 
result.set( sum); 
context.write( key, result); 
} 
}

public static void main( String[] args) throws Exception { 
Configuration conf = new Configuration();
Job job = Job.getInstance( conf, "word count");

job.setJarByClass(ChCount.class);

FileInputFormat.addInputPath( job, new Path("input")); 
FileOutputFormat.setOutputPath( job, new Path("output")); 
job.setMapperClass( WordCountMapper.class); 
job.setCombinerClass( WordCountReducer.class); 
job.setReducerClass( WordCountReducer.class);

job.setOutputKeyClass( Text.class); 
job.setOutputValueClass( IntWritable.class);

System.exit( job.waitForCompletion( true) ? 0 : 1); 
} 
}