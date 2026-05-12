package Query1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Emite (PROVINCIA\tGRADO_SEVERIDAD, 1) y (PROVINCIA\tTOTAL, 1) por cada registro.
 */
public class Query1Mapper1 extends Mapper<LongWritable, Text, Text, IntWritable> {

    private static final IntWritable ONE = new IntWritable(1);

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        String[] c = value.toString().split("\t");
        if (c.length < 21) return;

        // Verificar que sea fila de datos (PK_REGISTRO numérico)
        try { Integer.parseInt(c[0].trim()); } catch (NumberFormatException e) { return; }

        String provincia  = c[12].trim();
        String severidad  = c[7].trim();

        if (provincia.isEmpty() || severidad.isEmpty()) return;
        if (!severidad.equals("LEV") && !severidad.equals("MOD") && !severidad.equals("SEV")) return;

        context.write(new Text(provincia + "\t" + severidad), ONE);
        context.write(new Text(provincia + "\tTOTAL"),        ONE);
    }
}
