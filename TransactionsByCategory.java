package TDE2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.BasicConfigurator;
import java.io.IOException;

public class TransactionsByCategory {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        BasicConfigurator.configure();  // Configura o logger do log4j

        Configuration c = new Configuration();  // Cria a configuração do Hadoop

        // Analisa os argumentos de entrada
        String[] files = new GenericOptionsParser(c, args).getRemainingArgs();
        Path input = new Path("in/operacoes_comerciais_inteira.csv");  // Caminho do arquivo de entrada
        Path output = new Path("output/transactions_by_category");  // Caminho do diretório de saída

        Job j = Job.getInstance(c, "transactions-by-category");  // Cria o job com nome "transactions-by-category"
        j.setJarByClass(TransactionsByCategory.class);           // Define a classe principal
        j.setMapperClass(MapForTransactionsByCategory.class);    // Define a classe Mapper
        j.setReducerClass(ReduceForTransactionsByCategory.class); // Define a classe Reducer

        // Define os tipos de saída do Mapper (chave: categoria, valor: 1)
        j.setMapOutputKeyClass(Text.class);
        j.setMapOutputValueClass(IntWritable.class);

        // Define os tipos de saída do Reducer (chave: categoria, valor: total de transações)
        j.setOutputKeyClass(Text.class);
        j.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(j, input);  // Define o caminho do arquivo de entrada
        FileOutputFormat.setOutputPath(j, output);  // Define o diretório de saída

        System.exit(j.waitForCompletion(true) ? 0 : 1);  // Submete o job e encerra o programa com status
    }

    /**
     * Mapper que processa as transações e emite a categoria da transação como chave.
     */
    public static class MapForTransactionsByCategory extends Mapper<LongWritable, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);  // Valor constante 1 para contar transações
        private Text category = new Text();  // Objeto Text que armazenará a categoria

        /**
         * Método map que processa cada linha do arquivo de entrada.
         * @param key O offset da linha (não utilizado)
         * @param value O conteúdo da linha (cada transação)
         * @param context O objeto que permite emitir pares chave-valor
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String linha = value.toString();  // Converte a linha de entrada para string
            String[] col = linha.split(";");  // Divide a linha em colunas, usando ";" como delimitador

            // Verifica se há uma coluna suficiente para a categoria (supondo que a categoria esteja na última coluna)
            if (col.length > 8) {
                String categoria = col[9];  // A última coluna contém a categoria

                // Define a categoria como chave
                category.set(categoria);
                context.write(category, one);  // Emite a categoria e o valor 1
            }
        }
    }

    /**
     * Reducer que soma o número de transações por categoria.
     */
    public static class ReduceForTransactionsByCategory extends Reducer<Text, IntWritable, Text, IntWritable> {

        /**
         * Método reduce que processa todas as ocorrências de transações por categoria e calcula o total.
         * @param key A categoria emitida pelo Mapper
         * @param values Uma lista de valores (sempre 1) emitidos pelo Mapper
         * @param context O objeto que permite emitir a chave-valor final
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int soma = 0;  // Inicializa a soma das transações

            // Itera sobre todos os valores (sempre 1) e acumula a soma
            for (IntWritable val : values) {
                soma += val.get();  // Adiciona o valor à soma
            }

            // Emite a categoria e o total de transações
            context.write(key, new IntWritable(soma));
        }
    }
}
