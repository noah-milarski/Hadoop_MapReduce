package TDE2;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Classe personalizada AverageWritable para armazenar a soma e a contagem das transações.
 */
public class AverageWritable implements Writable {

    private float sum;  // Soma das transações
    private int count;  // Contagem de transações

    /**
     * Construtor vazio padrão (necessário para serialização)
     */
    public AverageWritable() {
    }

    /**
     * Construtor parametrizado que inicializa a soma e a contagem.
     * @param sum Soma das transações
     * @param count Contagem de transações
     */
    public AverageWritable(float sum, int count) {
        this.sum = sum;
        this.count = count;
    }

    public float getSum() {
        return sum;
    }

    public void setSum(float sum) {
        this.sum = sum;
    }

    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }

    /**
     * Método de serialização: escreve os dados do objeto para saída.
     * @param dataOutput O fluxo de saída
     */
    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeFloat(sum);  // Serializa a soma
        dataOutput.writeInt(count);  // Serializa a contagem
    }

    /**
     * Método de desserialização: lê os dados do objeto a partir da entrada.
     * @param dataInput O fluxo de entrada
     */
    @Override
    public void readFields(DataInput dataInput) throws IOException {
        sum = dataInput.readFloat();  // Lê a soma
        count = dataInput.readInt();  // Lê a contagem
    }
}
