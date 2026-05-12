package Query2;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/** Acumula suma_meses y conteo por (GENERO × SEVERIDAD). Salida: suma_meses\tconteo */
public class Query2Reducer1 extends Reducer<Text, Text, Text, Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        double sumMeses = 0;
        long   count    = 0;

        for (Text val : values) {
            String[] p = val.toString().split("\t");
            if (p.length < 2) continue;
            try {
                sumMeses += Double.parseDouble(p[0]);
                count    += Long.parseLong(p[1]);
            } catch (NumberFormatException e) { /* ignorar */ }
        }

        context.write(key, new Text(sumMeses + "\t" + count));
    }
}
