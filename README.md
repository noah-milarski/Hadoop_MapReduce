# Implementação de MapReduce

Este projeto é uma implementação de um job MapReduce no Hadoop, do grupo do Pedro Noah, Angelo Sequinel, Thomas Pickler, João Dadas e Vitor Bertoldi; que utiliza (em casos mais complexos) uma classe customizada que implementa a interface `Writable` para armazenar dados complexos. O objetivo é processar dados de entrada, filtrar registros e calcular o resultado, agrupando-os pelo critério que foi definido.

## Estrutura do Projeto

O projeto é dividido em quatro componentes principais:

1. **Classe Customizada Writable**
2. **Classe Mapper**
3. **Classe Reducer**
4. **Classe Principal (Driver)**

---

### 1. Classe Customizada Writable (Situacional)

Esta classe é usada para armazenar dados complexos (ex: soma e contagem de valores). Ela implementa a interface `Writable` do Hadoop, permitindo a serialização e desserialização dos dados durante o processo MapReduce.

#### Passos para Implementação:

- Definir variáveis de instância, como `float sum` e `int count`, para armazenar os valores.
- Implementar o método `write(DataOutput out)` para serializar os dados.
- Implementar o método `readFields(DataInput in)` para desserializar os dados.
- Incluir métodos *getters* e *setters* para acessar e modificar os valores.
- Criar um construtor vazio (obrigatório para a serialização) e um construtor parametrizado para inicializar os valores.

**Exemplo:**

```java
public class CustomWritable implements Writable {
    private float sum;
    private int count;

    public CustomWritable() {} // Construtor vazio

    public CustomWritable(float sum, int count) {
        this.sum = sum;
        this.count = count;
    }

    public void write(DataOutput out) throws IOException {
        out.writeFloat(sum);
        out.writeInt(count);
    }

    public void readFields(DataInput in) throws IOException {
        sum = in.readFloat();
        count = in.readInt();
    }

    // Getters e Setters
}
```
## 2. Classe Mapper

O Mapper é responsável por processar os dados de entrada linha por linha, filtrando e emitindo pares chave-valor (ex: ano e um objeto Writable com soma e contagem).

### Passos para Implementação:

- Definir o tipo da chave e valor de saída do Mapper (ex: `Text` para ano como chave e `CustomWritable` como valor).
- No método `map`, ler e dividir as linhas do arquivo de entrada com base em um delimitador (como vírgula ou ponto e vírgula).
- Filtrar os registros com base em condições específicas (ex: país ou tipo de transação).
- Emitir a chave (ex: o ano) e o valor (objeto `CustomWritable` com soma e contagem).

**Exemplo:**

```java
public class TransactionMapper extends Mapper<LongWritable, Text, Text, CustomWritable> {

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split(",");
        String country = fields[3];
        String year = fields[1];
        float transactionAmount = Float.parseFloat(fields[4]);

        // Filtrar registros (exemplo: país específico)
        if (country.equals("Brazil")) {
            context.write(new Text(year), new CustomWritable(transactionAmount, 1));
        }
    }
} 
```
## 3. Classe Reducer

O Reducer é responsável por agregar os valores associados a cada chave (ex: somar todas as transações de um determinado ano) e calcular a média.

### Passos para Implementação:

- Definir o tipo da chave e valor de saída do Reducer (ex: `Text` para o ano e `FloatWritable` para a média).
- No método `reduce`, iterar pela lista de valores (`CustomWritable`), acumulando as somas e as contagens.
- Calcular a média e emitir a chave (ex: o ano) e o valor (a média).

**Exemplo:**

```java
public class TransactionReducer extends Reducer<Text, CustomWritable, Text, FloatWritable> {

    public void reduce(Text key, Iterable<CustomWritable> values, Context context) throws IOException, InterruptedException {
        float sum = 0;
        int count = 0;

        for (CustomWritable val : values) {
            sum += val.getSum();
            count += val.getCount();
        }

        float average = sum / count;
        context.write(key, new FloatWritable(average));
    }
}
```
## 4. Classe Principal (Driver)

A classe principal é responsável por configurar e iniciar o job MapReduce.

### Passos para Implementação:

- Configurar o job com a configuração e nome do job (`Job job = Job.getInstance(conf, "job-name")`).
- Definir as classes do Mapper, Reducer e a classe `Writable`.
- Configurar as classes de chave e valor de saída do Mapper e Reducer.
- Definir os caminhos de entrada e saída, utilizando `FileInputFormat` e `FileOutputFormat`.
- Executar o job (`job.waitForCompletion(true)`).

**Exemplo:**

```java
public class TransactionDriver {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "transaction-average");

        job.setJarByClass(TransactionDriver.class);
        job.setMapperClass(TransactionMapper.class);
        job.setReducerClass(TransactionReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(CustomWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FloatWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
```