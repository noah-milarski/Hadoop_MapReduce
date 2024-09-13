package TDE2;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Classe personalizada TransactionWritable que implementa a interface Writable
 * para armazenar o valor mínimo e máximo de transações.
 */
public class TransactionWritable implements Writable {

    private float minValue;  // Valor da transação mais barata
    private float maxValue;  // Valor da transação mais cara

    /**
     * Construtor vazio padrão (necessário para serialização)
     */
    public TransactionWritable() {
    }

    /**
     * Construtor parametrizado que inicializa os valores mínimo e máximo.
     * @param minValue O valor da transação mais barata
     * @param maxValue O valor da transação mais cara
     */
    public TransactionWritable(float minValue, float maxValue) {
        this.minValue = minValue;
        this.maxValue = maxValue;
    }

    public float getMinValue() {
        return minValue;
    }

    public void setMinValue(float minValue) {
        this.minValue = minValue;
    }

    public float getMaxValue() {
        return maxValue;
    }

    public void setMaxValue(float maxValue) {
        this.maxValue = maxValue;
    }

    /**
     * Método de serialização: escreve os dados do objeto para saída.
     * @param dataOutput O fluxo de saída
     */
    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeFloat(minValue);  // Serializa o valor mínimo
        dataOutput.writeFloat(maxValue);  // Serializa o valor máximo
    }

    /**
     * Método de desserialização: lê os dados do objeto a partir da entrada.
     * @param dataInput O fluxo de entrada
     */
    @Override
    public void readFields(DataInput dataInput) throws IOException {
        minValue = dataInput.readFloat();  // Lê o valor mínimo
        maxValue = dataInput.readFloat();  // Lê o valor máximo
    }

    /**
     * Fornece uma representação legível dos valores mínimo e máximo.
     * @return A string representando o valor mínimo e máximo
     */
    @Override
    public String toString() {
        return "Min: " + minValue + ", Max: " + maxValue;
    }
}
