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

public class TransactionsByFlow {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        BasicConfigurator.configure();  // Configura o logger do log4j

        Configuration c = new Configuration();  // Cria a configuração do Hadoop

        // Analisa os argumentos de entrada
        String[] files = new GenericOptionsParser(c, args).getRemainingArgs();
        Path input = new Path("in/operacoes_comerciais_inteira.csv");  // Caminho do arquivo de entrada
        Path output = new Path("output/transactions_by_flow");  // Caminho do diretório de saída

        Job j = Job.getInstance(c, "transactions-by-flow");  // Cria o job com nome "transactions-by-flow"
        j.setJarByClass(TransactionsByFlow.class);           // Define a classe principal
        j.setMapperClass(MapForTransactionsByFlow.class);    // Define a classe Mapper
        j.setReducerClass(ReduceForTransactionsByFlow.class); // Define a classe Reducer

        // Define os tipos de saída do Mapper (chave: fluxo, valor: 1)
        j.setMapOutputKeyClass(Text.class);
        j.setMapOutputValueClass(IntWritable.class);

        // Define os tipos de saída do Reducer (chave: fluxo, valor: total de transações)
        j.setOutputKeyClass(Text.class);
        j.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(j, input);  // Define o caminho do arquivo de entrada
        FileOutputFormat.setOutputPath(j, output);  // Define o diretório de saída

        System.exit(j.waitForCompletion(true) ? 0 : 1);  // Submete o job e encerra o programa com status
    }

    /**
     * Mapper que processa as transações e emite o tipo de fluxo (Flow) como chave.
     */
    public static class MapForTransactionsByFlow extends Mapper<LongWritable, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);  // Valor constante 1 para contar transações
        private Text flowType = new Text();  // Objeto Text que armazenará o tipo de fluxo

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

            // Verifica se há uma coluna suficiente para o tipo de fluxo (supondo que o fluxo esteja na coluna 4)
            if (col.length > 4) {
                String flow = col[4];  // A quinta coluna contém o tipo de fluxo (Export ou Import)

                // Define o tipo de fluxo como chave
                flowType.set(flow);
                context.write(flowType, one);  // Emite o tipo de fluxo e o valor 1
            }
        }
    }

    /**
     * Reducer que soma o número de transações por tipo de fluxo.
     */
    public static class ReduceForTransactionsByFlow extends Reducer<Text, IntWritable, Text, IntWritable> {

        /**
         * Método reduce que processa todas as ocorrências de transações por tipo de fluxo e calcula o total.
         * @param key O tipo de fluxo emitido pelo Mapper
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

            // Emite o tipo de fluxo e o total de transações
            context.write(key, new IntWritable(soma));
        }
    }
}
