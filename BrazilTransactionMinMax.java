package TDE2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.BasicConfigurator;
import TDE2.TransactionWritable;

import java.io.IOException;

/**
 * Classe principal para encontrar a transação mais cara e mais barata no Brasil em 2016 usando MapReduce.
 */
public class BrazilTransactionMinMax {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        BasicConfigurator.configure();  // Configura o logger do log4j

        Configuration c = new Configuration();

        String[] files = new GenericOptionsParser(c, args).getRemainingArgs();

        Path input = new Path("in/operacoes_comerciais_inteira.csv");  // Caminho para o arquivo CSV de entrada
        Path output = new Path("output/brazil_transactions_min_max");  // Caminho para o diretório de saída

        Job j = Job.getInstance(c, "brazil-transactions-min-max");

        j.setJarByClass(BrazilTransactionMinMax.class);
        j.setMapperClass(MapForBrazilMinMax.class);
        j.setReducerClass(ReduceForBrazilMinMax.class);

        j.setMapOutputKeyClass(Text.class);
        j.setMapOutputValueClass(FloatWritable.class);

        j.setOutputKeyClass(Text.class);
        j.setOutputValueClass(TransactionWritable.class);

        FileInputFormat.addInputPath(j, input);
        FileOutputFormat.setOutputPath(j, output);

        System.exit(j.waitForCompletion(true) ? 0 : 1);
    }

    /**
     * Classe Mapper que processa cada linha do arquivo CSV e emite o valor da transação no Brasil em 2016.
     */
    public static class MapForBrazilMinMax extends Mapper<LongWritable, Text, Text, FloatWritable> {

        private final static Text brazil2016 = new Text("Brazil_2016");  // Chave que representa o Brasil em 2016
        private FloatWritable transactionValue = new FloatWritable();  // Valor da transação

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String linha = value.toString();  // Converte a linha de entrada para uma string
            String[] col = linha.split(";");  // Divide a linha em colunas usando ";" como delimitador

            if (col.length == 10) {  // Verifica se a linha contém 10 colunas
                String country = col[0];  // A primeira coluna contém o país
                String year = col[1];     // A segunda coluna contém o ano
                float price;

                try {
                    price = Float.parseFloat(col[5]);  // Converte o preço para float
                } catch (NumberFormatException e) {
                    return;  // Ignora linhas com preços inválidos
                }

                if (country.equalsIgnoreCase("Brazil") && year.equals("2016")) {
                    transactionValue.set(price);  // Define o valor da transação
                    context.write(brazil2016, transactionValue);  // Emite a chave "Brazil_2016" com o valor da transação
                }
            }
        }
    }

    /**
     * Classe Reducer que calcula a transação mais cara e mais barata para o Brasil em 2016.
     */
    public static class ReduceForBrazilMinMax extends Reducer<Text, FloatWritable, Text, TransactionWritable> {

        private TransactionWritable result = new TransactionWritable();  // Objeto para armazenar o resultado final

        @Override
        public void reduce(Text key, Iterable<FloatWritable> values, Context context) throws IOException, InterruptedException {
            float minValue = Float.MAX_VALUE;  // Inicializa o valor mínimo com o maior valor possível
            float maxValue = Float.MIN_VALUE;  // Inicializa o valor máximo com o menor valor possível

            for (FloatWritable value : values) {  // Itera sobre todos os valores recebidos
                float price = value.get();  // Obtém o preço da transação
                if (price < minValue) {  // Se o preço for menor que o valor mínimo atual, atualiza
                    minValue = price;
                }
                if (price > maxValue) {  // Se o preço for maior que o valor máximo atual, atualiza
                    maxValue = price;
                }
            }

            result.setMinValue(minValue);  // Define o valor mínimo
            result.setMaxValue(maxValue);  // Define o valor máximo
            context.write(key, result);  // Emite a chave "Brazil_2016" e o resultado formatado
        }
    }
}
