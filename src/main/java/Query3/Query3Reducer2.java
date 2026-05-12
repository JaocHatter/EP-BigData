package Query3;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Calcula el índice de carga de severidad por Red de Salud:
 *   Índice = Σ(peso_i × cantidad_i) / Σ(cantidad_i)
 * Rango: 1.00 (toda leve) a 3.00 (toda severa).
 * Salida: RED → "TOTAL_CASOS=N | INDICE_SEVERIDAD=X.XX (1=Leve, 3=Severa)"
 */
public class Query3Reducer2 extends Reducer<Text, Text, Text, Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        double sumaPonderada = 0;
        long   totalCasos    = 0;

        for (Text val : values) {
            String[] p = val.toString().split("\t");
            if (p.length < 2) continue;
            try {
                sumaPonderada += Double.parseDouble(p[0]);
                totalCasos    += Long.parseLong(p[1]);
            } catch (NumberFormatException e) { /* ignorar */ }
        }

        if (totalCasos == 0) return;

        double indice = sumaPonderada / totalCasos;

        String resultado = String.format(
                "TOTAL_CASOS=%d | INDICE_SEVERIDAD=%.4f (1.0=todo_leve, 3.0=todo_severo)",
                totalCasos, indice);

        context.write(key, new Text(resultado));
    }
}
