/*
CREATE KEYSPACE ConsultaOpiniao WITH replication = {'class':'SimpleStrategy', 'replication_factor':1};
USE ConsultaOpiniao;

// Q1:
CREATE TABLE estabelecimento (id UUID, estabelecimento_nome TEXT, estabelecimento_descricao TEXT, avaliacao DOUBLE, latitude DOUBLE, longitude DOUBLE, PRIMARY KEY (id));

 SELECT estabelecimento_nome, latitude, longitude, tipo_estabelecimento_descricao FROM estabelecimento WHERE id=?; 

// Q2:
CREATE TABLE usuario (cpf TEXT, senha TEXT, usuario_nome TEXT, email TEXT, PRIMARY KEY (cpf));

 SELECT usuario_nome, senha, email FROM usuario WHERE cpf=?; 

// Q3:
CREATE TABLE avaliacao (id UUID, estabelecimento_nome TEXT, estabelecimento_descricao TEXT, avaliacao MAP<TEXT,DOUBLE>, PRIMARY KEY (id,estabelecimento_nome)) WITH CLUSTERING ORDER BY (estabelecimento_nome ASC);

 SELECT estabelecimento_nome, tipo_estabelecimento_descricao, usuario_nome FROM avaliacao WHERE id=?; 

// Q4:
CREATE TABLE table2 (id UUID, tipo INT, estabelecimento_nome TEXT, avaliacao LIST<TEXT>, PRIMARY KEY (id,tipo)) WITH CLUSTERING ORDER BY (tipo ASC);

 SELECT tipo, estabelecimento_nome FROM table2 WHERE id=?; 

*/

package Cassandra.servico.consultaopiniao;

import Cassandra.gis.db.Tupla;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.TupleValue;
import com.google.common.reflect.TypeToken;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;


public class BD {

	

	private Cluster cluster;
	private Session session;
	private String url="127.0.0.1";
	private String keyspace="consulta_opiniao";
	//private String keyspace="geom";
	
	public BD() {
		String ks= "CREATE KEYSPACE if not exists "+ keyspace
				+" WITH REPLICATION = "
				+ "{ 'class' : 'SimpleStrategy', 'replication_factor' : 1 };";
		
		//Q1: Quais estabelecimentos estão disponíveis para avaliação?onde estão? qual o tipo?
		String q1="CREATE TABLE if not exists  estabelecimento "
				+ "(estabelecimento_id UUID,"
				+ "estabelecimento_nome TEXT,"
				+ "estabelecimento_descricao TEXT,"
				+ "avaliacao DOUBLE,"
				+ "latitude DOUBLE,"
				+ "longitude DOUBLE, PRIMARY KEY (estabelecimento_id));";

		
		//Q2: Quais dados cadastrais do usuario?
		String q2="CREATE TABLE if not exists usuario "
				+ "(cpf TEXT,"
				+ "usuario_nome TEXT,"
				+ "senha TEXT,"
				+ "email TEXT,"
				+ "PRIMARY KEY (cpf));";

		
		//Q3: Quais são todas as avaliações feitas dado um estabelecimento? Qual a nota de cada avaliação? De quem é cada avaliação?
		String q3="CREATE TABLE if not exists avaliacao "
				+ "(estabelecimento_id UUID,"// para nao ter q pesquisar todas as avaliacoes para achar a desses estabelecimento
				+ "avaliacao_id UUID,"
				+ "estabelecimento_nome TEXT,"
				+ "estabelecimento_descricao TEXT,"
				+ "usuario_nome TEXT,"
				+ "avaliacao DOUBLE,"
				+ "PRIMARY KEY (estabelecimento_id,avaliacao_id));";
				//+ "WITH CLUSTERING ORDER BY (usuario_nome ASC);";
	
		//Q3 v2: Quais são todas as avaliações feitas dado um estabelecimento? Qual a nota de cada avaliação? De quem é cada avaliação?
				@SuppressWarnings("unused")
				String q3_2="CREATE TABLE if not exists avaliacao "
						+ "(estabelecimento_id UUID,"
						+ "estabelecimento_nome TEXT,"
						+ "estabelecimento_descricao TEXT,"
						+ "avaliacao list<frozen<tuple<TEXT,DOUBLE>>>,"
						+ "PRIMARY KEY (estabelecimento_id));";
						//+ "WITH CLUSTERING ORDER BY (estabelecimento_nome ASC);";
						
		
		//Q4: Quais perguntas um estabelecimento faz ao usuário?
		String q4="CREATE TABLE if not exists tipoAvaliacao"
				+ "(estabelecimento_id UUID,"
				//+ "tipo INT,"
				+ "estabelecimento_descricao TEXT,"
				+ "avaliacao_descricao LIST<TEXT>,"
				+ "PRIMARY KEY (estabelecimento_id));";
				//+ "WITH CLUSTERING ORDER BY (tipo ASC);";
		
		
		//String idx1="CREATE INDEX if not exists avl_index on avaliacao(avaliacao_id)";
		String idx1="CREATE CUSTOM INDEX if not exists avaliacao_index ON avaliacao (avaliacao_id) USING 'org.apache.cassandra.index.sasi.SASIIndex'";
		@SuppressWarnings("unused")
		String idx1_2="CREATE CUSTOM INDEX if not exists avaliacao_index ON avaliacao (avaliacao) USING 'org.apache.cassandra.index.sasi.SASIIndex'";
		
		String idx2="CREATE CUSTOM INDEX if not exists usuario_index on usuario(usuario_nome)  USING 'org.apache.cassandra.index.sasi.SASIIndex'";
		//String idx3="CREATE CUSTOM INDEX if not exists  estabelecimento_index on estabelecimento(estabelecimento_id)  USING 'org.apache.cassandra.index.sasi.SASIIndex'";
		//String idx3="";
		//String idx4="";
		

		
		cluster = Cluster.builder().addContactPoint(url).build();
	
		//Cluster.builder().addContactPoint("192.168.0.100");
		//Cluster.builder().addContactPoint("192.168.0.102");
		//Cluster.builder().addContactPoint("164.40.41.21");
		
		session=cluster.newSession();
		session.execute(ks);
		session = cluster.connect(keyspace);
		//deletar essas 4 linha
		/*session.execute("drop table estabelecimento;");
		session.execute("drop table   usuario;");
		session.execute("drop table  avaliacao;");
		session.execute("drop table   tipoavaliacao;");*/
		
		session.execute(q1);	
		session.execute(q2);	
		session.execute(q3);	
		session.execute(q4);	
		session.execute(idx1);	
		session.execute(idx2);
		//session.execute(idx3);
	}

	
	///INSERT
	public void inserirEstabelecimento(UUID estabelecimento_id, double latitude, double longitude, String estabelecimento_descricao, String estabelecimento_nome){
		//try{
		session.execute("INSERT INTO estabelecimento ( estabelecimento_id,estabelecimento_descricao, estabelecimento_nome, avaliacao, latitude, longitude) VALUES ("
		+String.valueOf(estabelecimento_id)
		+", '"+estabelecimento_descricao
		+"', '"+estabelecimento_nome
		+"', 0.0"
		+", "+String.valueOf(latitude)
		+", "+String.valueOf(longitude)+")");
	
		
}

	public void inserirEstabelecimento_2(UUID estabelecimento_id, double latitude, double longitude, String estabelecimento_descricao, String estabelecimento_nome){
		//try{
		session.execute("INSERT INTO estabelecimento ( estabelecimento_id,estabelecimento_descricao, estabelecimento_nome, avaliacao, latitude, longitude) VALUES ("
		+String.valueOf(estabelecimento_id)
		+", '"+estabelecimento_descricao
		+"', '"+estabelecimento_nome
		+"', 0.0"
		+", "+String.valueOf(latitude)
		+", "+String.valueOf(longitude)+")");
		
		//ja cria um registro de avaliacao vazio
		session.execute("INSERT INTO avaliacao( estabelecimento_id, estabelecimento_nome,estabelecimento_descricao,avaliacao) VALUES ("
				+String.valueOf(estabelecimento_id)
				+", '"+estabelecimento_nome
				+"', '"+estabelecimento_descricao
				+"', null)");
		
}	
	
	public void inserirUsuario(String cpf, String senha, String nome, String email){
		
		session.execute("INSERT INTO usuario (cpf, senha, usuario_nome, email) VALUES ('"
		+cpf
		+"', '"+senha		
		+"', '"+nome
		+"', '"+email+"')");
		
	}
	
	
	
	public void inserirAvaliacao(UUID estabelecimento_id,UUID avaliacao_id,String estabelecimento_nome, String estabelecimento_descricao,String usuario_nome,double valor){
		
		
		session.execute("INSERT INTO avaliacao( estabelecimento_id, avaliacao_id,estabelecimento_nome,estabelecimento_descricao, usuario_nome,avaliacao) VALUES ("
				+String.valueOf(estabelecimento_id)
				+","+String.valueOf(avaliacao_id)
				+", '"+estabelecimento_nome
				+"', '"+estabelecimento_descricao
				+"', '"+usuario_nome
				+"', "+String.valueOf(valor)+")");
		
}
	
	
	public void inserirAvaliacao(UUID estabelecimento_id,String usuario_nome,double valor){
		
			Estabelecimento e=buscarEstabelecimento(estabelecimento_id);
			
			session.executeAsync("INSERT INTO avaliacao( estabelecimento_id, estabelecimento_nome,estabelecimento_descricao, usuario_nome,avaliacao) VALUES ("
					+String.valueOf(estabelecimento_id)
					+", '"+e.getNome()
					+"', '"+e.getDescricao()
					+"', '"+usuario_nome
					+"', "+String.valueOf(valor)+")");
			
	}
	
	
	public void inserirAvaliacao_2(UUID estabelecimento_id,String usuario_nome,double valor){
		//talvez criar uma registro para cada avaliacao tenha mais performance
		
		//if(buscarAvaliacao(estabelecimento_id)!=null){
		session.execute("UPDATE avaliacao SET avaliacao = avaliacao + [('"+usuario_nome+"',"+String.valueOf(valor)+")] "
					+" WHERE estabelecimento_id = "+String.valueOf(estabelecimento_id));
					//+" AND estabelecimento_nome = '"+estabelecimento_nome+"'";
		//}
	}
			
	
		
	public void inserirTipoAvaliacao(UUID estabelecimento_id,  String estabelecimento_descricao, String avaliacao_descricao){
		//try{
		if(buscarTipoAvaliacao(estabelecimento_id))
			session.execute("UPDATE tipoAvaliacao SET avaliacao_descricao = avaliacao_descricao + ['"+avaliacao_descricao+"'] "
					+" WHERE estabelecimento_id = "+String.valueOf(estabelecimento_id));
					//+" AND tipo ="+tipo);
		else
			session.execute("INSERT INTO tipoAvaliacao (estabelecimento_id, estabelecimento_descricao, avaliacao_descricao) VALUES ("
			+String.valueOf(estabelecimento_id)
			//+", "+String.valueOf(tipo)
			+", '"+estabelecimento_descricao	
			+"', ['"+avaliacao_descricao+"'])");
	}

	
	
	

	//SELECT
	//estabelecimentos
	public Estabelecimento buscarEstabelecimento(UUID estabelecimento_id){
		ResultSet result= session.execute("SELECT estabelecimento_id, latitude, longitude,estabelecimento_descricao, estabelecimento_nome FROM estabelecimento "
				+ "WHERE estabelecimento_id="+String.valueOf(estabelecimento_id)+";");
	
	
		for (Row row : result) {
			System.out.format("%s %e %e %s %s\n", row.getUUID("estabelecimento_id"), row.getDouble("latitude"), row.getDouble("longitude"), row.getString("estabelecimento_descricao"), row.getString("estabelecimento_nome"));
		
			Estabelecimento e= new Estabelecimento();
			e.setId(row.getUUID("estabelecimento_id"));
			e.setLatitude(row.getDouble("latitude"));
			e.setLongitude(row.getDouble("longitude"));
			e.setNome(row.getString("estabelecimento_nome"));
			e.setDescricao(row.getString("estabelecimento_descricao"));
			
			return e;
		}
		return null;
	}
	
	//usuarios
	public void buscarUsuario(String cpf){
		ResultSet results = session.execute("SELECT cpf, senha, usuario_nome, email FROM usuario "
				+ "WHERE cpf='"+cpf+"';");

		for (Row row : results) {
			System.out.format("%s %s %s %s\n", row.getString("cpf"), row.getString("senha"), row.getString("nome"), row.getString("email"));
		}

	}

		//retorna todas as avaliacaos
	public List<Avaliacao> buscarAvaliacao(UUID estabelecimento_id){
		ResultSet results = session.execute("SELECT * FROM avaliacao where estabelecimento_id="+estabelecimento_id+";");

		List<Avaliacao> L_av=new ArrayList<Avaliacao>();
		Avaliacao av=null;
		for (Row row : results) {
			
				/*String estabelecimento_nome;
				String estabelecimento_descricao;
				String usuario_nome;
				double nota;
	
				estabelecimento_nome=row.getString("estabelecimento_nome");
				estabelecimento_descricao=row.getString("estabelecimento_descricao");
				usuario_nome=row.getString("usuario_nome");
				nota=row.getDouble("avaliacao");*/
				av= new Avaliacao(estabelecimento_id,row.getUUID("avaliacao_id"),row.getString("estabelecimento_nome"),row.getString("estabelecimento_descricao"),row.getString("usuario_nome"),row.getDouble("avaliacao"));
				L_av.add(av);
		}
		return L_av;
	}
	
	public Tupla<String, Double> buscarAvaliacao2(UUID estabelecimento_id){
		ResultSet results = session.execute("SELECT avaliacao FROM avaliacao "
				+ "WHERE id="+estabelecimento_id+";");

		Tupla<String, Double> t=null;
		for (Row row : results) {
				String nome;
				double avaliacao;
				t= new Tupla<String, Double>();
			List<TupleValue> tuplas=row.getList("avaliacao",new TypeToken<TupleValue>(){
				private static final long serialVersionUID = 1L;});
			
				nome=tuplas.get(0).getString(0);
				avaliacao=tuplas.get(0).getDouble(1);
				t.put(nome, avaliacao);
				//tuplas.remove(0);
			
		
			//System.out.format("% %s\n", row.getMap("avaliacao", String.class, double.class));
		}
		return t;
	}
	
	

	//perguntas disponíveis em um estabelecimento
	public boolean buscarTipoAvaliacao(UUID estabelecimento_id){
		ResultSet results = session.execute("SELECT  estabelecimento_descricao,avaliacao_descricao FROM tipoAvaliacao "
				+ "WHERE estabelecimento_id="+estabelecimento_id+";");
		boolean has=false;
		for (@SuppressWarnings("unused") Row row : results) {
			has=true;
		//	System.out.format("%s %s\n", row.getString("estabelecimento_descricao"), row.getList("avaliacao_descricao",String.class));
		}
		return has;
	}

	
	//delete
	//update

}
