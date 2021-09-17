import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;


public class HBaseMapReduce {
  public static class hbaseMapper extends TableMapper<Text, IntWritable> {
    public void map(ImmutableBytesWritable rowKey, Result columns, Context context)
    throws IOException, InterruptedException {
      try {
        String inKey = new String(rowKey.get());
        String oKey = inKey.split("#")[0];
        byte[] bSales = columns.getValue(Bytes.toBytes("cf"), Bytes.toBytes("sales"));
        String sSales = new String(bSales);
        Integer sales = new Integer(sSales);
        context.write(new Text(oKey), new IntWritable(sales));
      } catch (RuntimeException e) {
        e.printStackTrace();
      }
    }
  }
  public static class hbaseReducer extends TableReducer<Text, IntWritable, ImmutableBytesWritable> {
    public void reduce(Text key, Iterable<IntWritable> values, Context context)
    throws IOException, InterruptedException {
      try {
        int sum = 0;
        for (IntWritable sales : values) {
          Integer intSales = new Integer(sales.toString());
          sum += intSales;
        }
        Put insHBase = new Put(key.getBytes());
        insHBase.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("sum"), Bytes.toBytes(sum));
        context.write(null, insHBase);
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
  }
  public static void main(String[] args) throws Exception {
    Configuration conf = HBaseConfiguration.create();
    // define scan and define column families to scan
    Scan scan = new Scan();
    scan.addFamily(Bytes.toBytes("cf"));
    Job job = Job.getInstance(conf);
    job.setJarByClass(HBaseMapReduce.class);
    // define input hbase table
    TableMapReduceUtil.initTableMapperJob("test1", scan, hbaseMapper.class, Text.class, IntWritable.class, job);
    // define output table
    TableMapReduceUtil.initTableReducerJob("test2", hbaseReducer.class, job);
    job.waitForCompletion(true);
  }
}
