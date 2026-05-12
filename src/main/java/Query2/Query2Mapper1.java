package Query2;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Normaliza la edad a meses (A×12, M, D/30) y emite:
 * clave: GENERO\tGRADO_SEVERIDAD
 * valor: edad_meses\t1
 */
public class Query2Mapper1 extends Mapper<LongWritable, Text, Text, Text> {

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        String[] c = value.toString().split("\t");
        if (c.length < 21) return;
        try { Integer.parseInt(c[0].trim()); } catch (NumberFormatException e) { return; }

        String genero    = c[2].trim();
        String tipoEdad  = c[4].trim();
        String severidad = c[7].trim();

        if (genero.isEmpty() || tipoEdad.isEmpty() || severidad.isEmpty()) return;
        if (!severidad.equals("LEV") && !severidad.equals("MOD") && !severidad.equals("SEV")) return;

        int edadRaw;
        try { edadRaw = Integer.parseInt(c[3].trim()); } catch (NumberFormatException e) { return; }

        double edadMeses;
        switch (tipoEdad) {
            case "A": edadMeses = edadRaw * 12.0;  break;
            case "M": edadMeses = edadRaw;          break;
            case "D": edadMeses = edadRaw / 30.0;  break;
            default:  return;
        }

        // Descartar edades fuera del rango pediátrico (0-60 meses = 0-5 años)
        if (edadMeses < 0 || edadMeses > 60) return;

        context.write(
            new Text(genero + "\t" + severidad),
            new Text(edadMeses + "\t1")
        );
    }
}
