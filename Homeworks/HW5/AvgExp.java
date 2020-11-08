import java.io.IOException;
import java.util.Map;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class AvgExp {

    /**private static String SIZE = "SIZE";
    private static String AVG = "AVG";

   /** private static Map<String, Float> getListCount (Iterable<FloatWritable> lifeexp) {
        Map<String, Float> resultMap = new HashMap<>();
        float avg = 0;
        float size = 0;
        for (FloatWritable le  : lifeexp) {
            size++;
            avg += le.get();
        }
        avg = avg/size;
        resultMap.put(AVG, avg);
        resultMap.put(SIZE, size);
        return resultMap;
    } */

	public static class AvgExpMapper extends Mapper<Object, Text, Text, FloatWritable> {
		
		@Override
        protected void map(Object offset, Text rows, Context context) throws IOException, InterruptedException {

            String[] cols = rows.toString().split(",");
            String continent = cols[2].replace("'", "");
            
            if (Float.parseFloat(cols[8].replace("'", "")) > 10000.00) {
                context.write(new Text(continent), new FloatWritable(Float.parseFloat(cols[7].replace("'", ""))));
            }

        }

	}

public static class AvgExpReducer
       extends Reducer<Text,FloatWritable,Text,FloatWritable> {
    private FloatWritable result = new FloatWritable();

    public void reduce(Text continent, Iterable<FloatWritable> lifeexp, 
                       Context context
                       ) throws IOException, InterruptedException {
      float avg = 0;
      int size= 0;
      for (FloatWritable val : lifeexp) {
        avg += val.get();
        size++;
      }
      avg = avg/size;
      result.set(avg);
      if (size>=5) {
        context.write(continent, result);
        }
    }
  }

public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        for (int i = 0; i < otherArgs.length; i++) {
            System.out.println(otherArgs[i]);
        }
        Job job = Job.getInstance(conf, "inf_hw5");
        job.setJarByClass(AvgExp.class);
        job.setMapperClass(AvgExpMapper.class);
        job.setReducerClass(AvgExpReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FloatWritable.class);

        for (int i = 0; i < otherArgs.length - 1; ++i) {
            FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
        }
        FileOutputFormat.setOutputPath(job,
                new Path(otherArgs[otherArgs.length - 1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
