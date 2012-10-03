package com.tumblr.rogueleaderr.hadoop_tutorial;

// code mostly taken from https://github.com/larsgeorge/hbase-book
// modified for distribution by George London on 10/2/2012. My contributions are released into the public domain.

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.io.ByteArrayInputStream;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.MultiTableOutputFormat;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.Statement;
import com.hp.hpl.jena.rdf.model.StmtIterator;

public class HBaseMapReduceExample {

  private static final Log LOG = LogFactory.getLog(HBaseMapReduceExample.class);
  public static final String NAME = "HBaseMapreduceExample"; // Define a job name for later use.
  public enum Counters { LINES }
	  
	static class ImportMapper // Define the mapper class, extending the provided Hadoop class.
	  extends Mapper<LongWritable, Text, ImmutableBytesWritable, Writable> { 

	    /**
	     * Maps the input.
	     *
	     * @param offset The current offset into the input file.
	     * @param line The current line of the file.
	     * @param context The task context.
	     * @throws IOException When mapping the input fails.
	     */

	    @Override
	    public void map(LongWritable offset, Text line, Context context) // The map() function transforms the key/value provided by the InputFormat to what is needed by the OutputFormat.
	    throws IOException {
	    	try {        
				// declare the table we'll be inserting into
	    		ImmutableBytesWritable parsedLinesTable = new ImmutableBytesWritable(Bytes.toBytes("parsed_lines"));
	    		// declare the column family and qualifiers we'll be working with
	    		byte[] termsFamily = Bytes.toBytes("terms");
	    		byte[] subjQualifier = Bytes.toBytes("subject");
				byte[] predQualifier = Bytes.toBytes("subject");
				byte[] objQualifier = Bytes.toBytes("subject");
	    		// keep a global counter to give each line a unique statement ID
				long sid = context.getCounter(Counters.LINES).getValue();
				byte[] byteSid = Bytes.toBytes(sid);
				// initialize rdf parsing machinery
				String lineString = line.toString();
				Model model = ModelFactory.createDefaultModel();
				InputStream streamIn = new ByteArrayInputStream(lineString.getBytes("UTF-8"));
				// parse the line
				model.read(streamIn, null, "N-TRIPLE");
				StmtIterator iter = model.listStatements();
				Statement stmt = iter.next();
				// get a subject, predicate, and object
				String subj = stmt.getSubject().toString();
				String pred = stmt.getPredicate().toString();
				String obj = stmt.getObject().toString();
				// convert to bytes to allow insertion
				byte[] byte_subj = Bytes.toBytes(subj);
				byte[] byte_pred = Bytes.toBytes(pred);
				byte[] byte_obj = Bytes.toBytes(obj);
				// declare the put operation to insert the data
				Put parsedLinePut = new Put(byteSid);
				// add values to put
				parsedLinePut.add(termsFamily, subjQualifier, byte_subj);
				parsedLinePut.add(termsFamily, predQualifier, byte_pred);
				parsedLinePut.add(termsFamily, objQualifier, byte_obj);
				// tell Hadoop it needs to perform the put on the table
				context.write(parsedLinesTable, parsedLinePut);
				// increment the counter
				context.getCounter(Counters.LINES).increment(1);
	    	} catch (Exception e) {
	        e.printStackTrace();
	    	}
	    }
	}

	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
		
		// "conf" objects hold the configuration properties for your database or hdfs 
		Configuration hbaseConf = HBaseConfiguration.create();
		// HBaseHelper contains utility scripts to manipulate HBase tables
		HBaseHelper helper;
		helper = HBaseHelper.getHelper(hbaseConf);
		// Declare the path to our input file
		Path exampleInputFile = new Path("/user/hdfs/input/sample_rdf.nt");
		// drop old tables
		helper.dropTable("parsed_lines");
	    // recreate them
		helper.createTable("parsed_lines", "terms"); // rows are sids
	    // create the Hadoop job
	    Job job = new Job(hbaseConf, "HBase w/ Map Reduce Example");
	    // configure the job
	    job.setJarByClass(HBaseMapReduceExample.class);
	    job.setMapperClass(ImportMapper.class);
	    job.setOutputKeyClass(ImmutableBytesWritable.class);
	    job.setOutputValueClass(Writable.class);
	    // tell Hadoop that the output will be writes to multipe tables (though in this case we're only using one.)
	    job.setOutputFormatClass(MultiTableOutputFormat.class);
	    // This is a map only job, therefore tell the framework to bypass the reduce step.
	    job.setNumReduceTasks(0); 
	    FileInputFormat.addInputPath(job, exampleInputFile);
	    // wait for the job to finish, then exit
	    boolean comp = job.waitForCompletion(true);
		LOG.info("Job Finished!"); // logs (by default) are in /var/log/hadoop-0.20-mapreduce/userlogs/
	    System.exit(comp ? 0 : 1);    
	}
}
