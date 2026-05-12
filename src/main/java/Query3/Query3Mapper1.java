package Query3;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Emite (RED\tSEVERIDAD, CANTIDAD) para acumular casos por severidad dentro de cada red.
 */
public class Query3Mapper1 extends Mapper<LongWritable, Text, Text, Text> {

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        String[] c = value.toString().split("\t");
        if (c.length < 21) return;
        try { Integer.parseInt(c[0].trim()); } catch (NumberFormatException e) { return; }

        String red       = c[15].trim();
        String severidad = c[7].trim();

        if (red.isEmpty() || severidad.isEmpty()) return;
        if (!severidad.equals("LEV") && !severidad.equals("MOD") && !severidad.equals("SEV")) return;

        int cantidad;
        try { cantidad = Integer.parseInt(c[11].trim()); } catch (NumberFormatException e) { cantidad = 1; }

        context.write(new Text(red + "\t" + severidad), new Text(String.valueOf(cantidad)));
    }
}
