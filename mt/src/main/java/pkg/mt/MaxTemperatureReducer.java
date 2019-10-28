package pkg.mt;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class MaxTemperatureReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    public void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException{
        int valormaximo = Integer.MIN_VALUE;
        for (IntWritable value : values){
            valormaximo = Math.max(valormaximo, value.get());
        }
        context.write(key, new IntWritable(valormaximo));
    }
}