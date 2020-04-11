import java.io.IOException;
import java.util.StringTokenizer;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.time.LocalDateTime;
import java.time.DayOfWeek;
import java.lang.Exception;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class AvgPassenger_Q4A {

    public static class AveragePassengerPerHourMapper extends Mapper<Object, Text, Text, DoubleWritable> {

        private final static DoubleWritable one = new DoubleWritable(1);
        private Text word = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            word.set(value);
            String nextLine = word.toString();
            String[] columns = nextLine.split(",");

            Text passengerKey = new Text("Passenger");

            Text tripKey = new Text("Trip");

            if (!columns[0].trim().isEmpty()) {

                // formatter for datetime
                DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

                try {
                    // datetime column converted to datetime
                    LocalDateTime date = LocalDateTime.parse(columns[1], formatter);

                    String hourVal;
                    if (date.getHour() < 10) {
                        hourVal = "0" + Integer.toString(date.getHour());
                    } else {
                        hourVal = Integer.toString(date.getHour());
                    }

                    // map hour_passemgerkey to number of passengers to calculate per hour
                    context.write(new Text(hourVal + "_" + passengerKey),
                            new DoubleWritable(Double.parseDouble(columns[3])));

                    // map hour_Trip to 1 to find total trips in the hour
                    context.write(new Text(hourVal + "_" + tripKey), one);

                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }

    // Get intermediate passenger count for each key, and intermediate trip count.
    // This is done at each split.
    // Combiner used to summarize mapped records with the same key
    public static class AveragePassengerPerHourCombiner extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
        private DoubleWritable result = new DoubleWritable();

        public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
                throws IOException, InterruptedException {
            double count = 0.0;
            for (DoubleWritable value : values) {
                count += value.get();
            }
            result.set(count);
            context.write(key, result);
        }
    }

    public static class AveragePassengerPerHourReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

        private DoubleWritable result = new DoubleWritable();
        double totPassenger = 0.0;

        public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
                throws IOException, InterruptedException {

            System.out.println(key);
            double sum = 0.0;

            // If "Trip" is present in the key, calculate the total number of trips per
            // hour, the
            // average and write it to output
            if (key.find("Trip") > 0) {
                for (DoubleWritable val : values) {
                    sum += val.get();
                }

                String[] pkey = key.toString().split("_");
                context.write(new Text(pkey[0] + "_Passenger_Average"), new DoubleWritable(totPassenger / sum));

            }

            else {
                // else calculate total passenger count per hour
                for (DoubleWritable val : values) {
                    sum += val.get();
                }
                totPassenger = sum;
            }

        }
    }

    // remaining
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "average passenger per hour");
        job.setJarByClass(AvgPassenger_Q4A.class);
        job.setMapperClass(AveragePassengerPerHourMapper.class);
        job.setCombinerClass(AveragePassengerPerHourCombiner.class);
        job.setReducerClass(AveragePassengerPerHourReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
