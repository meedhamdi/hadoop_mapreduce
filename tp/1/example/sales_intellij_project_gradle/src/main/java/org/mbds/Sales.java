package org.mbds;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

// Driver class (implements the main method).
public class Sales {

    // Mapper class.
    public static class SalesMap extends Mapper<LongWritable, Text, Text, Text> {

        private static final int REGION = 0;
        private static final int COUNTRY = 1;
        private static final int ITEM_TYPE = 2;
        private static final int SALES_CHANNEL = 3;
        private static final int UNITS_SOLD = 8;
        private static final int TOTAL_PROFIT = 13;

        private static Text group = new Text();
        private static Text groupValue = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // We skip the first header line of the csv file.
            if (key.get() == 0){
                return;
            }
            String analyseType = context.getConfiguration().get("org.mbds.analyseType");
            String[] row = value.toString().split(",");
            String col = "";
            String total = row[TOTAL_PROFIT];
            if (analyseType.startsWith("region")) {
                col = row[REGION];
            } else if (analyseType.startsWith("pays")) {
                col = row[COUNTRY];
            } else if (analyseType.startsWith("type_item")) {
                col = row[ITEM_TYPE];
            }
            if (analyseType.endsWith("_by_channel")) {
                col += " channel: " + row[SALES_CHANNEL];
                total += "," + row[UNITS_SOLD];
            }
            group.set(col);
            groupValue.set(total);
            context.write(group, groupValue);
        }
    }

    // Reducer class.
    public static class SalesReduce extends Reducer<Text, Text, Text, Text> {

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            float profit = 0;
            int unitsSold = 0;
            for (Text value : values) {
                String[] items = value.toString().split(",");
                profit += Float.parseFloat(items[0]);
                if (items.length == 2) {
                    unitsSold += Integer.valueOf(items[1]);
                }
            }
            String total = "Total Profit: " + String.format("%.2f", profit);
            if (unitsSold > 0) {
                total += ", Units sold: " + unitsSold;
            }
            context.write(key, new Text(total));
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        // Instantiate the Hadoop Configuration.
        Configuration conf = new Configuration();

        // Parse command-line arguments.
        // The GenericOptionParser takes care of Hadoop-specific arguments.
        String[] ourArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        // Check input arguments.
        if (ourArgs.length != 3) {
            System.err.println("Usage: sales <in> <out> <analyseType>");
            System.exit(2);
        }
        // We save the content of the third argument into the Hadoop Configuration object.
        conf.set("org.mbds.analyseType", ourArgs[2]);

        Job job = Job.getInstance(conf, "Sales");
        // Setup the Driver/Mapper/Reducer classes.
        job.setJarByClass(Sales.class);
        job.setMapperClass(SalesMap.class);
        job.setReducerClass(SalesReduce.class);
        // Indicate the key/value output types we are using in our Mapper & Reducer.
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        // Indicate from where to read input data from HDFS.
        FileInputFormat.addInputPath(job, new Path(ourArgs[0]));
        // Indicate where to write the results on HDFS.
        FileOutputFormat.setOutputPath(job, new Path(ourArgs[1]));

        // We start the MapReduce Job execution (synchronous approach).
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
