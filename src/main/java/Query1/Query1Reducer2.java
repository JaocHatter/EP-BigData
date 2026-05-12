package Query1;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Calcula el porcentaje de cada grado de severidad respecto al total provincial.
 * Salida: PROVINCIA → "TOTAL=N | LEV=XX.XX% | MOD=XX.XX% | SEV=XX.XX%"
 */
public class Query1Reducer2 extends Reducer<Text, Text, Text, Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        int total = 0, sev = 0, mod = 0, lev = 0;

        for (Text val : values) {
            String[] partes = val.toString().split(":");
            if (partes.length < 2) continue;
            int cnt;
            try { cnt = Integer.parseInt(partes[1]); } catch (NumberFormatException e) { continue; }

            switch (partes[0]) {
                case "TOTAL": total = cnt; break;
                case "SEV":   sev   = cnt; break;
                case "MOD":   mod   = cnt; break;
                case "LEV":   lev   = cnt; break;
            }
        }

        if (total == 0) return;

        double pLev = (double) lev / total * 100.0;
        double pMod = (double) mod / total * 100.0;
        double pSev = (double) sev / total * 100.0;

        String resultado = String.format(
                "TOTAL=%d | LEV=%.2f%% | MOD=%.2f%% | SEV=%.2f%%",
                total, pLev, pMod, pSev);

        context.write(key, new Text(resultado));
    }
}
