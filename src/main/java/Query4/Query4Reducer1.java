package Query4;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Por cada hoja (grupo_etario|género), cuenta cuántos casos son LEV, MOD, SEV
 * y calcula la impureza Gini: Gini = 1 - (pLEV² + pMOD² + pSEV²).
 *
 * Salida: grupo|género → "LEV=X,MOD=Y,SEV=Z,TOTAL=N,GINI=0.XXXX,MAJORITY=CLASE"
 */
public class Query4Reducer1 extends Reducer<Text, Text, Text, Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        int lev = 0, mod = 0, sev = 0;

        for (Text val : values) {
            switch (val.toString().trim()) {
                case "LEV": lev++; break;
                case "MOD": mod++; break;
                case "SEV": sev++; break;
            }
        }

        int total = lev + mod + sev;
        if (total == 0) return;

        double pLev = (double) lev / total;
        double pMod = (double) mod / total;
        double pSev = (double) sev / total;
        double gini = 1.0 - (pLev * pLev + pMod * pMod + pSev * pSev);

        // Clase mayoritaria
        String majority;
        int maxCount = Math.max(lev, Math.max(mod, sev));
        if (maxCount == sev) majority = "SEV";
        else if (maxCount == mod) majority = "MOD";
        else majority = "LEV";

        String dist = String.format(
                "LEV=%d,MOD=%d,SEV=%d,TOTAL=%d,GINI=%.4f,MAJORITY=%s",
                lev, mod, sev, total, gini, majority);

        context.write(key, new Text(dist));
    }
}
