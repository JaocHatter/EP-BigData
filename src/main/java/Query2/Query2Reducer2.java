package Query2;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Calcula edad promedio final en meses y años para cada grupo (GENERO × SEVERIDAD).
 * Salida: GENERO\tSEVERIDAD → "N=X | Prom=XX.XX meses (XX.XX años)"
 */
public class Query2Reducer2 extends Reducer<Text, Text, Text, Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        double totalSuma  = 0;
        long   totalCount = 0;

        for (Text val : values) {
            String[] p = val.toString().split("\t");
            if (p.length < 2) continue;
            try {
                totalSuma  += Double.parseDouble(p[0]);
                totalCount += Long.parseLong(p[1]);
            } catch (NumberFormatException e) { /* ignorar */ }
        }

        if (totalCount == 0) return;

        double promMeses = totalSuma / totalCount;
        double promAnios = promMeses / 12.0;

        String resultado = String.format(
                "N=%d | Prom=%.2f meses (%.2f años)",
                totalCount, promMeses, promAnios);

        context.write(key, new Text(resultado));
    }
}
