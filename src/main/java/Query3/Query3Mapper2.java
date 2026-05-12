package Query3;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Lee la salida de Job1: RED\tSEVERIDAD\tCOUNT
 * Aplica pesos (LEV=1.0, MOD=2.0, SEV=3.0) y re-emite agrupando por RED.
 * Valor: "peso_ponderado\tcantidad_total"
 */
public class Query3Mapper2 extends Mapper<LongWritable, Text, Text, Text> {

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        String[] p = value.toString().split("\t");
        if (p.length < 3) return;

        String red       = p[0].trim();
        String severidad = p[1].trim();
        long   cantidad;
        try { cantidad = Long.parseLong(p[2].trim()); } catch (NumberFormatException e) { return; }

        double peso;
        switch (severidad) {
            case "LEV": peso = 1.0; break;
            case "MOD": peso = 2.0; break;
            case "SEV": peso = 3.0; break;
            default: return;
        }

        double pesoPonderado = peso * cantidad;
        // Valor: "peso_ponderado\tcantidad" para que Reducer2 calcule el índice
        context.write(new Text(red), new Text(pesoPonderado + "\t" + cantidad));
    }
}
