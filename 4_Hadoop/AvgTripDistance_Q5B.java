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

public class AvgTripDistance_Q5B {

    public static class AverageTripDistancePerHourMapper extends Mapper<Object, Text, Text, DoubleWritable> {

        private final static DoubleWritable one = new DoubleWritable(1);
        private Text word = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            word.set(value);
            String nextLine = word.toString();
            String[] columns = nextLine.split(",");

            Text tripDistanceKey = new Text("Distance");
            Text tripKey = new Text("Trip");

            if (!columns[0].trim().isEmpty()) {

                // formatter for datetime
                DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

                try {

                    // datetime column converted to datetime
                    LocalDateTime date = LocalDateTime.parse(columns[1], formatter);
                    DayOfWeek dayOfWeek = date.getDayOfWeek(); // get day of week

                    String hourVal;
                    if (date.getHour() < 10) {
                        hourVal = "0" + Integer.toString(date.getHour());
                    } else {
                        hourVal = Integer.toString(date.getHour());
                    }

                    if (dayOfWeek.toString() == "SATURDAY" || dayOfWeek.toString() == "SUNDAY") { // for weekends

                        // map hour_weekend_tripdistancekey to distance per hour
                        context.write(new Text(hourVal + "_Weekend_" + tripDistanceKey),
                                new DoubleWritable(Double.parseDouble(columns[4])));

                        // map day_weekend_Trip to 1 to find total trips in the hour for weekend
                        context.write(new Text(hourVal + "_Weekend_" + tripKey), one);

                    }

                    else { // for weekdays

                        // map hour_weekday_distancekey to distance per hour
                        context.write(new Text(hourVal + "_Weekday_" + tripDistanceKey),
                                new DoubleWritable(Double.parseDouble(columns[4])));

                        // map day_weekday_Trip to 1 to find total trips in the hour for weekday

                        context.write(new Text(hourVal + "_Weekday_" + tripKey), one);

                    }

                } catch (Exception e) {

                    e.printStackTrace();

                }

            }

        }

    }

    // Get intermediate trip distances for each key and intermediate trip count.
    // This is done at each split.
    // Combiner used to summarize mapped records with the same key
    public static class AverageTripDistancePerHourCombiner extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

        private DoubleWritable result = new DoubleWritable();

        public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
                throws IOException, InterruptedException {

            double count = 0;

            for (DoubleWritable value : values) {
                count += value.get();
            }

            result.set(count);
            context.write(key, result);

        }

    }

    public static class AverageTripDistancePerHourReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

        private DoubleWritable result = new DoubleWritable();

        double totTripDistance = 0;

        public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
                throws IOException, InterruptedException {

            double sum = 0;

            // If "Trip" is present in the key, calculate the total number of trips per
            // hour, the
            // average and write it to output

            if (key.find("Trip") > 0) {
                for (DoubleWritable val : values) {
                    sum += val.get();
                }

                context.write(new Text(key + "Distance_Average"), new DoubleWritable(totTripDistance / sum));

            }

            // else calculate total trip distance per hour

            else {
                for (DoubleWritable val : values) {
                    sum += val.get();
                }

                totTripDistance = sum;

            }

        }

    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "average trip distance per hour weekday weekend");
        job.setJarByClass(AvgTripDistance_Q5B.class);
        job.setMapperClass(AverageTripDistancePerHourMapper.class);
        job.setCombinerClass(AverageTripDistancePerHourCombiner.class);
        job.setReducerClass(AverageTripDistancePerHourReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }

}
