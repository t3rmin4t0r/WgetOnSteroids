package org.notmysock.util;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.hdfs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.filecache.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.mapreduce.lib.reduce.*;

import org.apache.commons.cli.*;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.*;

import java.io.*;
import java.nio.*;
import java.util.*;
import java.net.*;
import java.math.*;
import java.security.*;

public class WgetOnSteroids extends Configured implements Tool {

	private static final String CONFIG_PREFIX = WgetOnSteroids.class.getName();
	private static final String CONFIG_OUTDIR = CONFIG_PREFIX+".outdir";
	
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        int res = ToolRunner.run(conf, new WgetOnSteroids(), args);
        System.exit(res);
    }
    
    public Path genInput(String urls) throws Exception {
        long epoch = System.currentTimeMillis()/1000;
        Path dest = new Path("/tmp/urls-"+epoch+".txt");
        FileSystem fs = FileSystem.get(getConf());
        FSDataOutputStream out = fs.create(dest);
        FileInputStream in = new FileInputStream(new File(urls));
        IOUtils.copy(in, out);
        in.close();
        out.close();
        fs.deleteOnExit(dest);
        return dest;
    }


    @Override
    public int run(String[] args) throws Exception {
        String[] remainingArgs = new GenericOptionsParser(getConf(), args).getRemainingArgs();

        CommandLineParser parser = new BasicParser();
        getConf().setInt("io.sort.mb", 4);
        org.apache.commons.cli.Options options = new org.apache.commons.cli.Options();
        options.addOption("u","urls", true, "urls");
        options.addOption("d","dir", true, "dir");
        options.addOption("n","numrows", true, "numrows");
        CommandLine line = parser.parse(options, remainingArgs);

        if(!(line.hasOption("urls") && line.hasOption("dir"))) {
          HelpFormatter f = new HelpFormatter();
          f.printHelp("WgetOnSteroids", options);
          return 1;
        }

        int n = 1;

        if(line.hasOption("numrows")) {
          n = Integer.parseInt(line.getOptionValue("numrows"));
        }

        Path in = genInput(line.getOptionValue("urls"));
        Path out = new Path(line.getOptionValue("dir"));

        Configuration conf = getConf();
        conf.setInt("mapred.task.timeout",0);
        conf.setInt("mapreduce.task.timeout",0);
        conf.set(CONFIG_OUTDIR, out.toString());
        Job job = new Job(conf, "WgetOnSteroids");
        job.setJarByClass(getClass());
        job.setNumReduceTasks(0);
        job.setMapperClass(WgetDownloader.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setInputFormatClass(NLineInputFormat.class);
        NLineInputFormat.setNumLinesPerSplit(job, n);

        FileInputFormat.addInputPath(job, in);
        FileOutputFormat.setOutputPath(job, out);

        // use multiple output to only write the named files
        LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);
        boolean success = job.waitForCompletion(true);        

        return 0;
    }   
   
    static final class WgetDownloader extends Mapper<LongWritable,Text, Text, Text> {
      FileSystem fs;
      protected void setup(Context context) throws IOException {
    	  fs = FileSystem.get(context.getConfiguration());
      }
      protected void cleanup(Context context) throws IOException, InterruptedException {
      }
      protected void map(LongWritable offset, Text command, Mapper.Context context) 
        throws IOException, InterruptedException {        
        String[] cmd = command.toString().split(" ");
        try {
				URI src = new URI(cmd[0]);
				String dst = FilenameUtils.getName(src.getPath());
				if (cmd.length > 1) {
					dst = cmd[1];
				}
				String outdir = context.getConfiguration().get(CONFIG_OUTDIR);
				Path destfile = new Path(String.format("%s/%s", outdir, dst));
				if(!fs.exists(destfile.getParent())) {
					fs.mkdirs(destfile.getParent());
				}
				System.err.println("Downloading " + src + " into " + destfile);
				FSDataOutputStream hdfs = fs.create(destfile);
				InputStream http = src.toURL().openStream();
				IOUtils.copyLarge(http, hdfs);
				hdfs.flush();
				hdfs.close();
			} catch (URISyntaxException uie) {
				throw new IOException(uie);
			}
      }
    }
}

