import java.io.IOException;
import java.util.StringTokenizer;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.time.LocalDateTime;
import java.time.DayOfWeek;
import java.lang.Exception;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class AvgPassenger_Q1 {

    public static class AveragePassengerMapper extends Mapper<Object, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            word.set(value);
            String nextLine = word.toString();
            String[] columns = nextLine.split(",");

            Text passengerKey = new Text("Total_Passenger");

            Text tripKey = new Text("Total_Trip");

            if (!columns[0].trim().isEmpty()) {

                // formatter for datetime
                DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

                try {

                    // datetime column converted to datetime
                    LocalDateTime date = LocalDateTime.parse(columns[1], formatter);

                    // map passengerkey to number of passengers
                    context.write(passengerKey, new IntWritable(Integer.parseInt(columns[3])));

                    // map tripkey to 1 to find total trips
                    context.write(tripKey, new IntWritable(1));

                    DayOfWeek dayOfWeek = date.getDayOfWeek();

                    // map DAY_PASSENGER to number of passenger to find total passengers in the
                    // day
                    // eg. THURSDAY_Passenger
                    Text daysKey = new Text(dayOfWeek + "_Passenger");
                    context.write(daysKey, new IntWritable(Integer.parseInt(columns[3])));

                    // map days_tripkey to 1 to find total trips in the day
                    context.write(new Text(dayOfWeek + "_Trip"), new IntWritable(1));

                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }

    // Get intermediate passenger count for each key, and intermediate trip count.
    // This is done at each split.
    // Combiner used to summarize mapped records with the same key
    public static class AveragePassengerCombiner extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int count = 0;
            for (IntWritable value : values) {
                count += value.get();
            }
            result.set(count);
            context.write(key, result);
        }
    }

    public static class AveragePassengerReducer extends Reducer<Text, IntWritable, Text, FloatWritable> {

        private IntWritable result = new IntWritable();
        float totPassenger = 0;

        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;

            // If "Trip" is present in the key, calculate the total number of trips, the
            // average and write it to output
            if (key.find("Trip") > 0) {
                for (IntWritable val : values) {
                    sum += val.get();
                }

                // _Trip
                // MONDAY_Trip
                String[] pkey = key.toString().split("_");

                context.write(new Text(pkey[0] + "_Passenger_Average"), new FloatWritable(totPassenger / sum));

            }

            else { // else calculate total number of Passengers

                for (IntWritable val : values) {
                    sum += val.get();
                }
                totPassenger = sum;
            }

        }
    }

    // remaining
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "average passenger");
        job.setJarByClass(AvgPassenger_Q1.class);
        job.setMapperClass(AveragePassengerMapper.class);
        job.setCombinerClass(AveragePassengerCombiner.class);
        job.setReducerClass(AveragePassengerReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
