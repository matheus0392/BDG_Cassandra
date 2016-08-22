package Cassandra.servico.consultaopiniao;

import java.util.UUID;

public class Avaliacao {

	
	private UUID est_id;
	private UUID ava_id;
	private String est_nome;
	private String est_descricao;
	private String usuario_nome;
	private double nota;
	
	public Avaliacao( UUID est_id,UUID ava_id,String est_nome,String est_descricao,String usuario_nome,double nota){
		this.est_id=est_id;
		this.ava_id=ava_id;
		this.est_nome=est_nome;
		this.est_descricao=est_descricao;
		this.usuario_nome=usuario_nome;
		this.nota=nota;
	}
	
	public UUID getEst_id() {
		return est_id;
	}



	public void setEst_id(UUID est_id) {
		this.est_id = est_id;
	}



	public UUID getAva_id() {
		return ava_id;
	}



	public void setAva_id(UUID ava_id) {
		this.ava_id = ava_id;
	}

	public String getEst_nome() {
		return est_nome;
	}


	public void setEst_nome(String est_nome) {
		this.est_nome = est_nome;
	}


	public String getEst_descricao() {
		return est_descricao;
	}


	public void setEst_descricao(String est_descricao) {
		this.est_descricao = est_descricao;
	}


	public String getUsuario_nome() {
		return usuario_nome;
	}


	public void setUsuario_nome(String usuario_nome) {
		this.usuario_nome = usuario_nome;
	}


	public double getNota() {
		return nota;
	}


	public void setNota(double nota) {
		this.nota = nota;
	}

}
