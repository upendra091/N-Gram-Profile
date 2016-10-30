
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
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
 

public class bigram 
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
    	String temp="";
       String currentWord;

       String line1 = value.toString();
       String newline = line1.toUpperCase();
       StringTokenizer tok = new StringTokenizer(newline);
        
        while(temp!=null && tok.hasMoreElements())
        {
        	currentWord = tok.nextToken();
        	currentWord = currentWord.replaceAll("[^A-Z0-9]", "");
        	if(currentWord.equals(""))
        	{
        		continue ;
        	}
        	String key=temp+" "+currentWord+" "+year;
        	context.write(new Text(key) , new Text("2"));
        	temp=currentWord;
        	
        }
    }
    
}
    public static class ureduce extends Reducer< Text ,Text , Text , Text > 
    {
        public void reduce(Text key, Iterable <Text> values, Context context ) throws IOException , InterruptedException 
        {
            
            int sum = 0;
            String count=null;
            for( Text val : values ) 
            {
                sum++ ;
            }
            
            context.write( key , new Text(sum+""));
        } 
        }
    
    public static void main(String[] args) throws Exception
    {
    	Configuration conf=new Configuration();
    	Job job=new Job(conf,"bigrams count");
    	job.setJarByClass(bigram.class);
    	job.setMapperClass(umap.class);
        job.setReducerClass(ureduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
      }
}

    