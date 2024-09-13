package TDE2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.BasicConfigurator;

import java.io.IOException;

/**
 * Classe principal para calcular o valor médio das transações por ano somente no Brasil usando MapReduce.
 */
public class BrazilTransactionAverage {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        BasicConfigurator.configure();  // Configura o logger do log4j

        Configuration c = new Configuration();

        String[] files = new GenericOptionsParser(c, args).getRemainingArgs();

        Path input = new Path("in/operacoes_comerciais_inteira.csv");  // Caminho para o arquivo CSV de entrada
        Path output = new Path("output/brazil_transactions_average");  // Caminho para o diretório de saída

        Job j = Job.getInstance(c, "brazil-transactions-average");

        j.setJarByClass(BrazilTransactionAverage.class);
        j.setMapperClass(MapForBrazilAverage.class);
        j.setReducerClass(ReduceForBrazilAverage.class);

        j.setMapOutputKeyClass(Text.class);
        j.setMapOutputValueClass(AverageWritable.class);

        j.setOutputKeyClass(Text.class);
        j.setOutputValueClass(FloatWritable.class);

        FileInputFormat.addInputPath(j, input);
        FileOutputFormat.setOutputPath(j, output);

        System.exit(j.waitForCompletion(true) ? 0 : 1);
    }

    /**
     * Classe Mapper que processa cada linha do arquivo CSV e emite a soma e a contagem das transações.
     */
    public static class MapForBrazilAverage extends Mapper<LongWritable, Text, Text, AverageWritable> {

        private final static Text brazilKey = new Text("Brazil");  // Chave que representa o Brasil
        private AverageWritable transactionData = new AverageWritable();  // Objeto para armazenar soma e contagem

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

                if (country.equalsIgnoreCase("Brazil")) {
                    transactionData.setSum(price);
                    transactionData.setCount(1);  // Emite a soma e contagem de 1 transação
                    context.write(new Text(year), transactionData);  // Emite o ano como chave
                }
            }
        }
    }

    /**
     * Classe Reducer que calcula a média das transações por ano para o Brasil.
     */
    public static class ReduceForBrazilAverage extends Reducer<Text, AverageWritable, Text, FloatWritable> {

        private FloatWritable averageResult = new FloatWritable();  // Objeto para armazenar a média

        @Override
        public void reduce(Text key, Iterable<AverageWritable> values, Context context) throws IOException, InterruptedException {
            float totalSum = 0;  // Inicializa a soma total
            int totalCount = 0;  // Inicializa a contagem total

            for (AverageWritable value : values) {  // Itera sobre todos os valores recebidos
                totalSum += value.getSum();  // Soma os valores
                totalCount += value.getCount();  // Soma as contagens
            }

            // Calcula a média das transações
            float average = totalCount == 0 ? 0 : totalSum / totalCount;
            averageResult.set(average);  // Define o resultado da média
            context.write(key, averageResult);  // Emite o ano e a média das transações
        }
    }
}
