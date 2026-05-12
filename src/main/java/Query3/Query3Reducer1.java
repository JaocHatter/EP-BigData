package Query3;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/** Suma la CANTIDAD de casos por (RED × SEVERIDAD). */
public class Query3Reducer1 extends Reducer<Text, Text, Text, Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        long totalCantidad = 0;
        for (Text val : values) {
            try { totalCantidad += Long.parseLong(val.toString().trim()); }
            catch (NumberFormatException e) { /* ignorar */ }
        }
        context.write(key, new Text(String.valueOf(totalCantidad)));
    }
}
