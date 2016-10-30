package assignment;

import java.io.*;
import java.util.HashSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;

import java.util.StringTokenizer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
 

public class unigram 
{
public static class umap extends Mapper<LongWritable ,Text , Text , Text>
{
    public HashSet<String> h=new HashSet<String>();
    int begin=0;
    String year;
    boolean skipline=false;
    
    public void map(LongWritable key , Text value , Context context) throws IOException , InterruptedException
    {
       
        FileSplit fileSplit = (FileSplit)context.getInputSplit();
         String fileName = fileSplit.getPath().getName();
        if(!h.contains(fileName))
        {
           
            h.add(fileName);
            begin=0;
            skipline=false;
            skipLines(context,value);
        }
        else
        {   
            if(skipline==true&&begin==1)
            {
                    Count(context,value,fileName);
            }
            else
            {
                skipLines(context,value);
            }
        }
        
    }
   
    public void skipLines(Context context,Text value)
    {
        int x=0;
        String line = value.toString();
        if(line.startsWith("Release Date:"))
        {
            StringTokenizer st=new StringTokenizer(line);
            while(st.hasMoreElements())
            {   
                String tokens=st.nextToken();
                x++;
                int a = 0 ;
                if(x==4)
                {
                	try
                	{
                		Integer.parseInt(tokens);
                	}
                	catch(Exception e )
                	{
                		a = 1 ;
                	}
                	
                }
                if(a==1)
                {
                	x--;
                	continue;
                }
                if(x==4)
                    year=tokens;
            }
            skipline=true;
        }
        else if(line.startsWith("*** START"))
        {
            begin=1;
        }
    }
    public void Count(Context context,Text value,String fileName) throws IOException, InterruptedException
    {
        String line1 = value.toString();
       
        String line_new = line1.toUpperCase();
        StringTokenizer tok = new StringTokenizer(line_new);
       
        while(tok.hasMoreTokens())
        {
            String str=tok.nextToken().replaceAll("[^A-Z0-9]", "");
            String key=str+" "+year;
            String values=fileName+" "+1;
            context.write(new Text(key) , new Text(values));
           
        }
    }
    
}
    public static class ureduce extends Reducer< Text ,Text , Text , Text > 
    {
        public void reduce(Text key, Iterable <Text> values, Context context ) throws IOException , InterruptedException 
        {
            HashSet<String> h1= new HashSet<String>();
            int sum = 0;
            String count=null;
            for( Text val : values ) 
            {
            	sum = sum+1 ;
            }
            
            context.write( key , new Text(sum+""));
        } 
        }
    
    public static void main(String[] args) throws Exception
    {
    	Configuration conf=new Configuration();
    	Job job=new Job (conf,"unigrams count");
    	job.setJarByClass(unigram.class);
    	job.setMapperClass(umap.class);
        job.setReducerClass(ureduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
      }
}

    