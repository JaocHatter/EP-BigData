package Query2;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Lee la salida de Job1: GENERO\tSEVERIDAD\tSUMA_MESES\tCONTEO
 * Re-emite la misma clave compuesta para que Reducer2 calcule el promedio global.
 */
public class Query2Mapper2 extends Mapper<LongWritable, Text, Text, Text> {

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        // Formato de línea Job1: "GENERO\tSEVERIDAD\tSUMA\tCONTEO"
        String[] p = value.toString().split("\t");
        if (p.length < 4) return;

        String genero    = p[0].trim();
        String severidad = p[1].trim();
        String suma      = p[2].trim();
        String conteo    = p[3].trim();

        context.write(
            new Text(genero + "\t" + severidad),
            new Text(suma + "\t" + conteo)
        );
    }
}
