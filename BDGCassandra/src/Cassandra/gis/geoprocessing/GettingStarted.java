package Cassandra.gis.geoprocessing;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;


import org.json.JSONException;

import com.esri.core.geometry.Geometry;
import com.esri.core.geometry.Line;
import com.esri.core.geometry.Point;
import com.esri.core.geometry.Point2D;
import com.esri.core.geometry.Polygon;
import com.esri.core.geometry.Polyline;
import com.esri.core.geometry.MultiPoint;
import com.esri.core.geometry.ProgressTracker;
import com.esri.core.geometry.SpatialReference;

import Cassandra.gis.db.BDgeo;
import Cassandra.gis.db.Geo;
import Cassandra.gis.db.Propriedade;
import Cassandra.servico.consultaopiniao.BD;
import Cassandra.servico.consultaopiniao.Estabelecimento;


public class GettingStarted {

	
	private static Funcoes func;
	private static BD BancoDados;
	
	public GettingStarted() {
		 
	}

	public static void main(String[] args){
	
		func= new Funcoes(new BDgeo("127.0.0.1", "geometry"));
		float antes=System.currentTimeMillis();
		System.out.println("antes: "+System.currentTimeMillis()/1000);
		
		
		
	/*	for(int i=0;i<12;i++){
			ImportarGeoJson("C:\\Users\\Acer\\Desktop\\testeGeoJson\\df\\df"+String.valueOf(i)+".geojson");
		}
		ExportarGeometria("C:\\Users\\Acer\\Desktop\\testeGeoJson\\df\\df.geojson");
		
		System.out.println((float)(System.currentTimeMillis()-antes)/1000+"segundos de busca no banco");
		System.out.println(System.currentTimeMillis()/1000+"segundos de exportação do banco");
		testeBD();
		testeOperadores();
		testeBDgeo();
		*/
		funcoes_geoespaciais();
		
		System.out.println((float)(System.currentTimeMillis()-antes)/1000+"segundos de inserção no banco");	
	}
	
	public static void testeBD(){
			
		BancoDados= new BD();
		int x;
		UUID y,z;
		List <UUID> yy = new ArrayList <UUID>();
		List <UUID> zz = new ArrayList <UUID>();
		
		
		y = UUID.randomUUID();yy.add(y);
		
		
		BancoDados.inserirEstabelecimento(y, -15.79225, -47.83938, "Hospital", "Minha localizacao");y = UUID.randomUUID();yy.add(y);
		
		BancoDados.inserirEstabelecimento(y, -15.767938, -47.874228, "Escola", "Centro de Ensino Medio Asa Norte");y = UUID.randomUUID();yy.add(y);
		BancoDados.inserirEstabelecimento(y, -15.808065, -47.909539, "Escola", "Centro de Ensino Medio Elefante Branco");y = UUID.randomUUID();yy.add(y);
		BancoDados.inserirEstabelecimento(y, -15.7830155, -47.938376, "Escola", "Centro Educacional 01 do Cruzeiro");y = UUID.randomUUID();yy.add(y);
		BancoDados.inserirEstabelecimento(y, -15.796561, -47.938454, "Escola", "Centro Educacional 02 do Cruzeiro Novo");y = UUID.randomUUID();yy.add(y);
		BancoDados.inserirEstabelecimento(y, -15.827537, -47.917211, "Escola", "Centro de Ensino Fundamental 04 de Brasilia");y = UUID.randomUUID();yy.add(y);
		BancoDados.inserirEstabelecimento(y, -15.75597,  -47.87861, "Escola", "Centro de Ensino Medio Paulo Freire");y = UUID.randomUUID();yy.add(y);
		BancoDados.inserirEstabelecimento(y, -15.82669, -47.92002, "Hospital", "Hospital Geral de Brasilia");y = UUID.randomUUID();yy.add(y);
		BancoDados.inserirEstabelecimento(y, -15.82382, -47.897158, "Hospital", "Hospital Regional da Asa Sul");y = UUID.randomUUID();yy.add(y);
		BancoDados.inserirEstabelecimento(y, -15.800837, -47.888676, "Hospital", "Hospital de Base do Distrito Federa");y = UUID.randomUUID();yy.add(y);
		BancoDados.inserirEstabelecimento(y, -15.772453, -47.873429, "Hospital", "Hospital Universitário de Brasilia");y = UUID.randomUUID();yy.add(y);
		BancoDados.inserirEstabelecimento(y, -15.785671, -47.882864, "Hospital", "Hospital Regional da Asa Norte");
		
		
		

		for(x=0;x<yy.size();x++){
			
			Estabelecimento est=BancoDados.buscarEstabelecimento(yy.get(x));
			
			if(est.getDescricao().equals("Escola")){
			
				BancoDados.inserirTipoAvaliacao (yy.get(x),"Escola", "Qual sua nota para o atendimento desta escola?");
				BancoDados.inserirTipoAvaliacao (yy.get(x),"Escola", "Qual sua nota para os profissionais desta escola?");
				BancoDados.inserirTipoAvaliacao (yy.get(x),"Escola", "Qual sua nota para a limpeza desta escola?");
				BancoDados.inserirTipoAvaliacao (yy.get(x),"Escola", "Qual sua nota para a organização desta escola?");
				BancoDados.inserirTipoAvaliacao (yy.get(x),"Escola", "Qual sua nota para a pontualidade dos profissionais desta escola?");

			}
			
			else if(est.getDescricao().equals("Hospital")){
				BancoDados.inserirTipoAvaliacao (yy.get(x),"Hospital", "Qual sua nota para o atendimento deste hospital?");
				BancoDados.inserirTipoAvaliacao (yy.get(x),"Hospital", "Qual sua nota para os profissionais deste hospital?");
				BancoDados.inserirTipoAvaliacao (yy.get(x),"Hospital", "Qual sua nota para a limpeza deste hospital?");
				BancoDados.inserirTipoAvaliacao (yy.get(x),"Hospital", "Qual sua nota para a organização deste hospital?");
				BancoDados.inserirTipoAvaliacao (yy.get(x),"Hospital", "Qual sua nota para a pontualidade dos profissionais deste hospital?");

				
			}
		}
		

			for(x=0;x<11;x++){

				z = UUID.randomUUID();
				zz.add(z);

				//BancoDados.inserirUsuario("000.111.222-"+String.valueOf(x), String.valueOf(x)+String.valueOf(x+1)+String.valueOf(x+2), "Matheus"+String.valueOf(x), "weddynny"+String.valueOf(x)+"@hotmail.com");
			}
			
			BancoDados.inserirUsuario("000.111.222-1","senha", "Matheus", "Matheus@hotmail.com");
			BancoDados.inserirUsuario("000.111.222-2","senha", "Joao", "Joao@hotmail.com");
			BancoDados.inserirUsuario("000.111.222-3","senha", "Maria", "Maria@hotmail.com");
			BancoDados.inserirUsuario("000.111.222-4","senha", "Joana", "Joana@hotmail.com");
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		long antes =System.currentTimeMillis();
		System.out.println("antes: "+antes);
		for(x=0;x<0;x++){
			
			BancoDados.inserirAvaliacao(yy.get(x%yy.size()),UUID.randomUUID(),"Escola n-"+String.valueOf(x),"Escola","Matheus"+String.valueOf(x),(((double)x+1)*0.5) % 5.0);
		}
		long depois =System.currentTimeMillis();
		System.out.println("antes: "+depois);
		System.out.println((float)(depois-antes)/1000+"segundos de inserçãono banco");
	
		
	}

	public static void testeOperadores(){
		
		System.out.println("start teste");
		func.InicializarTestes("C:\\Users\\Acer\\Desktop\\testeGeoJson\\teste.json");
		func.SalvarGeoJson("C:\\Users\\Acer\\Desktop\\testeGeoJson\\contem.json",func.testeContain());
		System.out.println("testeContain");
		func.SalvarGeoJson("C:\\Users\\Acer\\Desktop\\testeGeoJson\\cruza.json",func.testeCrosses());
		System.out.println("testeCrosses");
		func.SalvarGeoJson("C:\\Users\\Acer\\Desktop\\testeGeoJson\\disjunto.json",func.testeDisjoint());
		System.out.println("testeDisjoint");
		func.SalvarGeoJson("C:\\Users\\Acer\\Desktop\\testeGeoJson\\intersecta.json",func.testeIntersect());
		System.out.println("testeIntersect");
		func.SalvarGeoJson("C:\\Users\\Acer\\Desktop\\testeGeoJson\\sobrepoe.json",func.testeOverlap());
		System.out.println("testeOverlap");
		func.SalvarGeoJson("C:\\Users\\Acer\\Desktop\\testeGeoJson\\toca.json",func.testeTouch());
		System.out.println("testeTouch");
		func.SalvarGeoJson("C:\\Users\\Acer\\Desktop\\testeGeoJson\\dentro.json",func.testeWithin());
		System.out.println("testeWithin");
		
	}
	
	public static void testeBDgeo(){
			//testar o armazenamento de cada tipo de representação geografica
			/*	ponto								OK
			 * 	linha(1 linha : 2 pontos)			OK
			 *	poligono							OK
			 *	multiponto							OK
			 *	polilinha(1~m)(m linhas: n pontos)	OK
			 *	multipoligono						OK
			 */
			try{
		//banco
		BDgeo BancoDados= new BDgeo("127.0.0.1", "geometry");
		
		//variaveis de teste
		Point p= new Point(8.34,56.23);
		MultiPoint mp = new MultiPoint();
		Line l= new Line();
		Polyline pl = new Polyline();
		Polygon pg = new Polygon();
		Propriedade<String> propriedades= new Propriedade<String>();
		List<Geo> list_geo= new ArrayList<Geo>();
		Geo geo= new Geo();
	//	propriedades.put("País", "Brasil");
	//	propriedades.put("info","eu moro aki");
	//	BancoDados.InserirGeo((float)9.5, "minha casa", propriedades);
		
		geo.setGeometria(p);
		geo.setId("minha casa");
		geo.setPropriedades(propriedades);
		list_geo.add(geo);
		//ponto
		propriedades.put("País", "Brasil");
		propriedades.put("info","eu moro aki");
		BancoDados.InserirGeo(list_geo);
		list_geo.clear();
		
		geo.setGeometria(mp);
		geo.setId("meus vizinhos conhecidos");
		geo.setPropriedades(propriedades);
		list_geo.add(geo);
		//multiponto
		mp.add(new Point(9.0,9.0));
		mp.add(new Point(8.0,8.0));
		mp.add(new Point(7.0,7.0));
		mp.add(new Point(6.0,6.0));
		mp.add(new Point(5.0,5.0));
		propriedades.put("País", "Brasil");
		propriedades.put("info","eu moro aki");
		BancoDados.InserirGeo(list_geo);
		list_geo.clear();
	
		//Linha
		geo.setGeometria(l);
		geo.setId( "minha rua");
		geo.setPropriedades(propriedades);
		list_geo.add(geo);
		l.setStart(p);		
		l.setEnd(new Point(1.0, 1.0));
		propriedades.put("País", "Brasil");
		propriedades.put("info","eu moro aki");
		propriedades.put("comprimento de b1", String.valueOf(l.calculateLength2D()));
		BancoDados.InserirGeo(list_geo);
		list_geo.clear();
		
		geo.setGeometria(pl);
		geo.setId("meu quarteirão");
		geo.setPropriedades(propriedades);
		list_geo.add(geo);
		//polilinha
		pl.startPath(0.0,1.0);pl.lineTo(0.0,2.0);pl.lineTo(0.0,3.0);
		pl.startPath(1.0,1.0);pl.lineTo(1.0,2.0);pl.lineTo(1.0,3.0);
		pl.startPath(0.0,4.0);pl.lineTo(0.0,5.0);pl.lineTo(0.0,6.0);
		pl.startPath(1.0,4.0);pl.lineTo(1.0,5.0);pl.lineTo(1.0,6.0);
		propriedades.put("País", "Brasil");
		propriedades.put("info","eu moro aki");
		propriedades.put("comprimento: ",String.valueOf(pl.calculateLength2D()));
		BancoDados.InserirGeo(list_geo);
		list_geo.clear();
		
		geo.setGeometria(pg);
		geo.setId("minha quadra");
		geo.setPropriedades(propriedades);
		list_geo.add(geo);
		//poligono 
		pg.startPath(0.0,0.0);pg.lineTo(0.0,2.0);pg.lineTo(2.0,2.0);pg.lineTo(2.0,0.0);
		pg.startPath(2.0,0.0);pg.lineTo(2.0,2.0);pg.lineTo(4.0,2.0);pg.lineTo(4.0,0.0);
		propriedades.put("País", "Brasil");
		propriedades.put("info","eu moro aki");
		propriedades.put("comprimento: ",String.valueOf(pg.calculateLength2D()));
		propriedades.put("area: ",String.valueOf(pg.calculateArea2D()));
		BancoDados.InserirGeo(list_geo);
		list_geo.clear();	
		
			}
			catch(NullPointerException e){e.printStackTrace();}
		
	}


	public static void ImportarGeoJson(String path){
			
			try {
				List<Geo>	retorno =func.GeoJson2Geometry(path);
				func.ImportarBanco(retorno);
				System.out.println(path);
				
			} catch (IOException | JSONException  e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
			
	}
		
	public static void ExportarGeometria(String path){
			
			try {
				List<Geo>	retorno =func.ExportarBanco();
				System.out.println("fim da busca ");
				String resultado=func.Geometry2GeoJson(retorno, path);
				System.out.println("fim da exportação");
			} catch ( ClassNotFoundException | NullPointerException  e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
			
	}
		
		
		
	public static void  funcoes_geoespaciais(){
		//func.comprimento_total("highway","C:\\Users\\Acer\\Desktop\\testeGeoJson\\teste1.geojson");
		func.localizar_raio(-47.88322,-15.79401, 10000, "school", "C:\\Users\\Acer\\Desktop\\testeGeoJson\\teste2.geojson");
		try{
			//func.porcentagem_area("Universidade de Brasília - Campus Universitário Darcy Ribeiro","C:\\Users\\Acer\\Desktop\\testeGeoJson\\teste3.geojson");
		}catch(Exception f){}
	}
		
}
		
	