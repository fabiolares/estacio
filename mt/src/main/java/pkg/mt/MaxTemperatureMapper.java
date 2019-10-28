package pkg.mt;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MaxTemperatureMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
	private static final int MISSING = 9999;
	@Override
	public void map(LongWritable Key,Text value, Context context)
		throws IOException,InterruptedException{
		String linha = value.toString();
		String ano = linha.substring(15,19);
		
		int tempreaturaAr;
		if(linha.charAt(87) == '+'){
			tempreaturaAr = Integer.parseInt(linha.substring(88,92));
		}
		else{
			tempreaturaAr =  Integer.parseInt(linha.substring(87,92));
		}
		String qualidade = linha.substring(92,93);
		if(tempreaturaAr != MISSING && qualidade.matches("[01459]")){
			context.write(new Text(ano),new IntWritable(tempreaturaAr));
		}
	}
}
