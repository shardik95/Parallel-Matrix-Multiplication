package matrix;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

public class SparseHV extends Configured implements Tool {
    private static final Logger logger = LogManager.getLogger(SparseHV.class);

    public static class SparseMatrixMapper extends Mapper<Object, Text, Text, Text> {

        int H;
        Random rand;

        private final String COL_DELIMITER = ":";
        private final String DELIMITER = ",";
        private final String LEFT_MATRIX_ID = "L";


        /**
         * setup function to get Partition parameters from the driver
         *
         * @param context
         */
        @Override
        public void setup(Context context) {

            // H-partitioning over Left Matrix
            H = Integer.parseInt(context.getConfiguration().get("H"));
            rand = new Random();
        }

        /**
         * Map function to process each input record and emit to the relevant Reduce Task
         *
         * @param key key for each Map call
         * @param value the input record - MatrixId,rowId,colId:val,colId:val
         * @param context the context object
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        public void map(final Object key, final Text value, final Context context) throws IOException, InterruptedException {

            /*
            Split the input record into the corresponding relevant entities
            */

            String[] matrixRecord = value.toString().split(DELIMITER);
            String matrixId = matrixRecord[0];
            Text outputKey = new Text();
            Text outputValue = new Text();

            // if input record from the Left Matrix
            if (matrixId.equals(LEFT_MATRIX_ID)) {
                int rowId = Integer.parseInt(matrixRecord[1]);

                String[] columns = Arrays.copyOfRange(matrixRecord, 2, matrixRecord.length);

                // Select random integer from range (1,H)
                int randomRow = rand.nextInt(H) + 1;

                // for all columns in the row emit the randowRow as key,
                // we do not need to consider the partitions in right matrix
                // as there is only 1 partition since the right matrix is a vector
                for (String column : columns) {
                    String[] columnValuePair = column.split(COL_DELIMITER);
                    int columnId = Integer.parseInt(columnValuePair[0]);
                    double cellValue = Double.parseDouble(columnValuePair[1]);


                    outputKey.set(randomRow + "");
                    outputValue.set(matrixId + DELIMITER + rowId + DELIMITER + columnId + DELIMITER + cellValue);
                    context.write(outputKey, outputValue);
                }
            } else {
                int rowId = Integer.parseInt(matrixRecord[1]);
                String column = matrixRecord[2];
                String[] colValuePair = column.split(COL_DELIMITER);
                int columnId = Integer.parseInt(colValuePair[0]);
                double cellValue = Double.parseDouble(colValuePair[1]);

                // Emit tuples for all regions in the column
                for (int i = 1; i <= H; i++) {
                    outputKey.set(i + "");
                    outputValue.set(matrixId + DELIMITER + rowId + DELIMITER + columnId + DELIMITER + cellValue);
                    context.write(outputKey, outputValue);
                }
            }
        }
    }

    public static class SparseMatrixReducer extends Reducer<Text, Text, NullWritable, Text> {

        int Q;
        private final String DELIMITER = ",";
        private final String RIGHT_MATRIX_ID = "R";
        private final double ALPHA = 0.15;

        /**
         * setup function to get Partition parameters from the driver
         *
         * @param context
         */
        @Override
        public void setup(Context context) {
            // Q: no. of rows of the right matrix = no of cols of left matrix
            Q = Integer.parseInt(context.getConfiguration().get("Q"));
        }

        /**
         * Reduce function to process each input record and
         * calculate the matrix product for the corresponding row and columns
         *
         * @param key regionId of the partition
         * @param values the input record (regionID, [(matrixId,rowId,colId,cellValue),(...])
         * @param context the context object
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        public void reduce(final Text key, final Iterable<Text> values, final Context context) throws IOException, InterruptedException {

            // A HashMap to store the left matrix(transition matrix) indexed by the rowId
            // with values being (colId,cellValue) pair
            Map<Integer, List<String>> leftMatrixMap = new HashMap<>();
            // A vector to store the right Matrix(PageRank Values) indexed by the rowId
            double[] rightVector = new double[Q];


            for (Text value : values) {

                // Split input value into matrixId,rowId,colId,cellValue
                String[] tokens = value.toString().split(DELIMITER);
                String matrixId = tokens[0];
                int rowId = Integer.parseInt(tokens[1]) - 1;
                int columnId = Integer.parseInt(tokens[2]) - 1;
                double cellValue = Double.parseDouble(tokens[3]);

                // If matrixId==Right store value in vector for right Matrix
                // else store in the HashMap both indexed by rowID
                if (matrixId.equals(RIGHT_MATRIX_ID)) {
                    rightVector[rowId] = cellValue;
                } else {
                    List<String> currentRow = leftMatrixMap.getOrDefault(rowId, new ArrayList<>());
                    currentRow.add(columnId + DELIMITER + cellValue);
                    leftMatrixMap.put(rowId, currentRow);
                }
            }

            // Iterate over entries for each row in Left and compute the corresponding matrix product with right
            for (Map.Entry<Integer, List<String>> entry : leftMatrixMap.entrySet()) {
                int rowId = entry.getKey();

                List<String> columns = entry.getValue();
                double sum = 0.0;
                for (String column : columns) {
                    String[] columnValuePair = column.split(DELIMITER);
                    int columnId = Integer.parseInt(columnValuePair[0]);
                    double cellValue = Double.parseDouble(columnValuePair[1]);
                    sum += cellValue * rightVector[columnId];
                }

                sum = ALPHA/Q + (1-ALPHA)*sum;
                // Emit the matrix product for a (row,col) pair only if non-zero (Sparse matrix format)
                // as the output matrix will have only one column, we emit for every (row,1) pair with non-zero value
                if (sum != 0.0) {
                    context.write(NullWritable.get(), new Text("R," + (rowId + 1) + ",1:" + sum));
                }
            }
        }
    }

    @Override
    public int run(final String[] args) throws Exception {
        int totalIterations = Integer.parseInt(args[7]);
        Job job;
        Configuration jobConf;

        String rightVectorInput = args[1];
        String baseOutput = args[8];
        int jobCompletion = 0;

        for(int iteration = 0; iteration < totalIterations; iteration++) {
            jobConf = getConf();
            jobConf.set("mapreduce.output.textoutputformat.separator", "");
            jobConf.set("P", args[2]);
            jobConf.set("Q", args[3]);
            jobConf.set("R", args[4]);
            jobConf.set("H", args[5]);
            jobConf.set("V", args[6]);

            job = Job.getInstance(jobConf, "SparseHV");
            job.setJarByClass(SparseHV.class);

            MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, SparseMatrixMapper.class);
            MultipleInputs.addInputPath(job, new Path(rightVectorInput), TextInputFormat.class, SparseMatrixMapper.class);

            job.setReducerClass(SparseMatrixReducer.class);
            job.setMapOutputKeyClass(Text.class);
            job.setOutputKeyClass(NullWritable.class);
            job.setOutputValueClass(Text.class);

            String iterationOutput = baseOutput + "/iteration"+ iteration;
            FileOutputFormat.setOutputPath(job, new Path(iterationOutput));
            rightVectorInput = iterationOutput;
            jobCompletion = job.waitForCompletion(true) ? 0: 1;
        }
        return jobCompletion;
    }

    public static void main(final String[] args) {
        if (args.length != 9) {
            throw new Error("Nine arguments required");
        }

        try {
            ToolRunner.run(new SparseHV(), args);
        } catch (final Exception e) {
            logger.error("", e);
        }
    }

}