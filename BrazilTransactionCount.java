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

/**
 * Classe principal para contar o número de transações envolvendo o Brasil usando MapReduce.
 */
public class BrazilTransactionCount {

    /**
     * Método principal que configura e executa o job MapReduce.
     * @param args argumentos de linha de comando para especificar entrada e saída
     * @throws IOException
     * @throws ClassNotFoundException
     * @throws InterruptedException
     */
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        BasicConfigurator.configure();  // Configura o logger do log4j para evitar mensagens desnecessárias de advertência

        // Cria a configuração do Hadoop
        Configuration c = new Configuration();

        // Análise dos argumentos de entrada do usuário
        String[] files = new GenericOptionsParser(c, args).getRemainingArgs();

        // Caminho para o arquivo de entrada e o diretório de saída
        Path input = new Path("in/operacoes_comerciais_inteira.csv");  // Arquivo CSV com os dados
        Path output = new Path("output/brazil_transactions_number");   // Diretório para armazenar a saída

        // Cria e configura o job MapReduce
        Job j = Job.getInstance(c, "brazil-transactions");  // Cria um job com o nome "brazil-transactions"

        // Define a classe principal do job, mapper e reducer
        j.setJarByClass(BrazilTransactionCount.class);  // Classe que contém o job
        j.setMapperClass(MapForBrazilTransactions.class);  // Classe do Mapper
        j.setReducerClass(ReduceForBrazilTransactions.class);  // Classe do Reducer

        // Define o tipo de saída do Mapper (chave e valor)
        j.setMapOutputKeyClass(Text.class);  // Chave de saída do Mapper é Text (ex.: "Brazil")
        j.setMapOutputValueClass(IntWritable.class);  // Valor de saída do Mapper é IntWritable (contagem de 1)

        // Define o tipo de saída do Reducer (chave e valor)
        j.setOutputKeyClass(Text.class);  // Chave de saída do Reducer é Text (ex.: "Brazil")
        j.setOutputValueClass(IntWritable.class);  // Valor de saída do Reducer é IntWritable (soma total)

        // Define o caminho do arquivo de entrada e saída
        FileInputFormat.addInputPath(j, input);  // Adiciona o caminho do arquivo CSV de entrada
        FileOutputFormat.setOutputPath(j, output);  // Adiciona o diretório de saída para os resultados

        // Executa o job e encerra o programa com o status de sucesso (0) ou falha (1)
        System.exit(j.waitForCompletion(true) ? 0 : 1);
    }

    /**
     * Classe Mapper que processa as transações e emite "Brazil" como chave para cada transação envolvendo o Brasil.
     */
    public static class MapForBrazilTransactions extends Mapper<LongWritable, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);  // Valor constante 1 para contar cada transação
        private Text word = new Text();  // Objeto Text que armazenará a chave ("Brazil")

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
            String linha = value.toString();  // Converte a linha de entrada (transação) para uma string
            String[] col = linha.split(";");  // Divide a linha em colunas, usando ";" como delimitador

            String pais = col[0];  // A primeira coluna contém o país

            // Verifica se o país da transação é o Brasil (ignora maiúsculas/minúsculas)
            if (pais.equalsIgnoreCase("Brazil")) {
                word.set(pais);  // Define "Brazil" como a chave
                context.write(word, one);  // Emite a chave "Brazil" com o valor 1
            }
        }
    }

    /**
     * Classe Reducer que recebe todas as ocorrências de "Brazil" e calcula a soma total.
     */
    public static class ReduceForBrazilTransactions extends Reducer<Text, IntWritable, Text, IntWritable> {

        /**
         * Método reduce que processa todas as ocorrências de "Brazil" e soma o total de transações.
         * @param key A chave "Brazil" emitida pelo Mapper
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

            // Emite a chave "Brazil" e o total de transações
            context.write(key, new IntWritable(soma));
        }
    }
}