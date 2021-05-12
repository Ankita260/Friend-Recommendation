import java.io.IOException;
import java.util.HashMap;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.ArrayList;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
public class FriendMapReduce {
	public static class FriendMapper extends Mapper<LongWritable, Text, Text, Text>{
		
		Text one = new Text("1");
		Text minusOne = new Text("-1");
        Text keyUser = new Text();
		Text suggTuple = new Text();
		Text existingFriend = new Text();
		String [] str,List;
		int i,j;
		
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			str = value.toString().split("\\s");
			if (str.length==1){
				str = null;
				return;
			}   
			List = str[1].split(",");
			for (i=0; i<List.length; i++) {
				keyUser.set(new Text(List[i]));
				for(j=0; j<List.length; j++){
					if(j==i) {
						continue;
					}
                     suggTuple.set(List[j] + ",1");
					context.write(keyUser, suggTuple);
				}
				existingFriend.set(str[0] + ",-1");
				context.write(keyUser, existingFriend);
				
			}

		}
	}
public static class FriendReducer extends Reducer<Text,Text,IntWritable,Text> {
		
		HashMap<String, Integer> recommended;
		String [] arrStr;
		int [] pair = new int [2];
		List<String> Vals;
        StringBuffer suggestion = new StringBuffer();
		StringBuffer tmp_str = new StringBuffer();
		String recList= new String();
		IntWritable x;
		
public void reduce(Text key, Iterable<Text> values,  Context context) throws IOException, InterruptedException {
			recommended = new HashMap<String, Integer>();
			suggestion = new StringBuffer();
			tmp_str = new StringBuffer();
			
			Vals = new ArrayList<String>();
			for (Text val_1 : values) {
				Vals.add(val_1.toString());
				
			}

			for (String val : Vals) {
				if(val.contains("-1")) {
					recommended.put(arrStr[0], new Integer(-1));
				}
				
				arrStr = val.toString().split(",");
                if(recommended.containsKey(arrStr[0])){
					if (recommended.get( arrStr[0]) != -1){
						recommended.put(arrStr[0],recommended.get(arrStr[0]) + 1);
					}
				}
				else {
					recommended.put(arrStr[0],1);
				}
				
			}

			recList = sortFriends(recommended);
			System.out.println(key + "\t" + recList);
			x = new IntWritable(Integer.parseInt(key.toString()));
			context.write(x, new Text(recList));
			
		}

public String sortFriends (HashMap a){
			Object[] recomm =  a.keySet().toArray();
			Object Count[] = a.values().toArray();
			
			int i, j;
Object temp, tempcount;
			StringBuffer returnlist = new StringBuffer();
			for(i=0; i<Count.length; i++) {
				for(j=0; j<Count.length-1 ; j++){
					if(Integer.parseInt(Count[j].toString()) < Integer.parseInt(Count[j+1].toString()) ) {
						
						tempcount = Count[j+1];
						Count [j+1] = Count[j];
						Count [j] = tempcount;

						temp = recomm[j+1];
						recomm [j+1] = recomm[j];
						recomm [j] = temp;
	 				}
				}
			}

			returnlist.append("\t");
			for(i=0; i<10 && i<recomm.length; i++) {
				returnlist.append(recomm[i].toString()).append(",");
			}
			 return returnlist.toString();
		}
	}
public static void main(String[] args) throws Exception {
	Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "People_You_May_Know");
    job.setJarByClass(FriendMapReduce.class);
    FileInputFormat.addInputPath(job, new Path("input"));
    FileOutputFormat.setOutputPath(job, new Path("output"));
    job.setMapperClass(FriendMapper.class);
    job.setReducerClass(FriendReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    
    System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
	
}
