package br.com.avroMode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import br.com.avroMode.avro.loja.*;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintStream;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;

@SpringBootApplication
public class AvroModeApplication implements CommandLineRunner {
	
	/*
	 * Antes de Executar ir na pasta src/main/avro onde tem Pedido.avsc
	 * e executar o comando abaixo para criar os arquivos java
	 * 
	 * java -jar avro-tools-1.9.1.jar compile schema Pedido.avsc  ../java/
	 *
	 *br.com.avroMode.avro.loja
	 *Endereco.java
	 *Pedido.java
	 *ProdutoResumo.java
	 *TipoEntrega.java
	 *
	 *Caso vc queira gerar outra vez e so apagar o arquivo loja inteiro 
	 *e rodar o comando acima outra vez
	 *
	 *Este foi criado pelo SpringBoot Initializ
	 * */

	private final Logger LOG = LoggerFactory.getLogger(this.getClass());

	private static final List<Pedido> PEDIDOS;

	static {
		final List<Pedido> pedidos = Arrays.asList(
				Pedido.newBuilder().setCodigoPedido(1L).setDataPedido(dateToLong(2019, 10, 2))
						.setTipoEntrega(TipoEntrega.FISICO).setEmailCliente(null)
						.setEnderecoEntrega(Endereco.newBuilder().setLogradouro("Rua Reta, 58")
								.setBairro("Jardim Gramado").setCidade("Jurupema").setEstado("GO").setCEP("75000-000")
								.setTelefone("(62) 99999-9999").build())
						.setProdutos(Arrays.asList(
								ProdutoResumo.newBuilder().setCodigo(1L).setNome("Liquidificador").setPreco("150,99")
										.setDescricao("Belo liquidificador").build(),
								ProdutoResumo.newBuilder().setCodigo(2L).setNome("Refrigerador").setPreco("1399,99")
										.setDescricao(null).build()))
						.build(),
				Pedido.newBuilder().setCodigoPedido(2L).setDataPedido(dateToLong(2019, 9, 15))
						.setTipoEntrega(TipoEntrega.DIGITAL).setEmailCliente("client1@pedido.com")
						.setEnderecoEntrega(null)
						.setProdutos(Arrays.asList(
								ProdutoResumo.newBuilder().setCodigo(1L).setNome("The Amazin Spiderman")
										.setPreco("59,99").build(),
								ProdutoResumo.newBuilder().setCodigo(2L).setNome("Assassin's Creed Odissey")
										.setPreco("199,99").build()))
						.build());

		PEDIDOS = new ArrayList<>();
		for (int i = 0; i < 10_000; i++) {
			PEDIDOS.addAll(pedidos);
		}
	}

	public static void main(String[] args) {
		SpringApplication.run(AvroModeApplication.class, args);
	}

	@Override
	public void run(String... args) throws Exception {
		/*
		 * Caso quiser testar sem passar parametros
		 */
		// String arg = "write";
		
		String arg = "read";
		
		String filename = "Pedidos.avro";

		try {
			if (arg.equals("read")) {
				readOrders(filename);
			} else {
				writeOrders(filename);
				writeOrdersJson();
			}
		} catch (IOException e) {
			print(System.err, "Erro ao processar arquivo: %s", e.getMessage());
			e.printStackTrace(System.err);
			System.exit(-1);
		}
	}

	private static void readOrders(String inFilename) throws IOException {
		DatumReader<Pedido> datumReader = new SpecificDatumReader<>(Pedido.class);

		long t0 = System.nanoTime();
		int counter = 0;
		Pedido sample = null;
		try (DataFileReader<Pedido> fileReader = new DataFileReader<>(new File(inFilename), datumReader)) {
			while (fileReader.hasNext()) {
				sample = fileReader.next();
				if (counter == 0) {
					print("Primeira iteracao em %.8fs", delta(t0));
				}
				counter += 1;
			}
		}

		print("%d registros lidos em %.3fs", counter, delta(t0));
		print("Exemplo de registro:%n%s", sample);
	}

	private static void writeOrders(String outFilename) throws IOException {
		DatumWriter<Pedido> datumWriter = new SpecificDatumWriter<>(Pedido.class);

		long t0 = System.nanoTime();
		try (DataFileWriter<Pedido> dataFileWriter = new DataFileWriter<>(datumWriter)) {
			dataFileWriter.setCodec(CodecFactory.deflateCodec(9));
			dataFileWriter.create(Pedido.SCHEMA$, new File(outFilename));
			for (Pedido pedido : PEDIDOS) {
				dataFileWriter.append(pedido);
			}
		}
		print("%d registros escritos em %.8fms", PEDIDOS.size(), delta(t0) * 1e-6);
	}

	private static void writeOrdersJson(){
		// Write JSON file
		try (FileWriter file = new FileWriter("orders.json")) {
			file.write(PEDIDOS.toString());
			file.flush();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/* FUNCOES UTILITARIAS */

	private static void print(String format, Object... args) {
		print(System.out, format, args);
	}

	private static void print(PrintStream out, String format, Object... args) {
		out.println(String.format(format, args));
	}

	private static double delta(long t0) {
		return (System.nanoTime() - t0) * 1e-9;
	}

	private static long dateToLong(int year, int month, int day) {
		LocalDate d = LocalDate.of(year, month, day);
		return d.atStartOfDay().atZone(ZoneId.of("UTC")).toInstant().toEpochMilli();
	}

}
