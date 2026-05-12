package Query1;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Lee la salida de Job1 (PROVINCIA\tSEVERIDAD\tCOUNT) y re-emite agrupando por PROVINCIA.
 * Valor: "SEVERIDAD:COUNT"
 */
public class Query1Mapper2 extends Mapper<LongWritable, Text, Text, Text> {

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        // Línea de Job1: "PROVINCIA\tSEVERIDAD\tCOUNT"
        String[] p = value.toString().split("\t");
        if (p.length < 3) return;

        String provincia = p[0].trim();
        String severidad = p[1].trim();
        String count     = p[2].trim();

        context.write(new Text(provincia), new Text(severidad + ":" + count));
    }
}
