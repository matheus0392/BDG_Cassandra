package Cassandra.gis.geoprocessing;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.List;

import org.json.JSONException;

import com.esri.core.geometry.Geometry;
import com.esri.core.geometry.Line;
import com.esri.core.geometry.MultiPoint;
import com.esri.core.geometry.OperatorContains;
import com.esri.core.geometry.OperatorCrosses;
import com.esri.core.geometry.OperatorDisjoint;
import com.esri.core.geometry.OperatorDistance;
import com.esri.core.geometry.OperatorExportToGeoJson;
import com.esri.core.geometry.OperatorImportFromGeoJson;
import com.esri.core.geometry.OperatorIntersects;
import com.esri.core.geometry.OperatorOverlaps;
import com.esri.core.geometry.OperatorTouches;
import com.esri.core.geometry.OperatorWithin;
import com.esri.core.geometry.Point;
import com.esri.core.geometry.Point2D;
import com.esri.core.geometry.Polygon;
import com.esri.core.geometry.Polyline;
import com.esri.core.geometry.ProgressTracker;
import com.esri.core.geometry.SpatialReference;

import Cassandra.gis.db.BDgeo;
import Cassandra.gis.db.Geo;
import Cassandra.gis.db.Propriedade;


//esta classe È respons·vel pelo processamento geoespacial
@SuppressWarnings("unused")
public class Funcoes {

	private BDgeo BancoDados;
	private Geo geo;
	
	public Funcoes(BDgeo bd){
		if (bd==null)
			throw new NullPointerException();
		BancoDados= bd;
	}
	/***
	 * 
	 * Aqui √© a parte para criar funÁıees especÌficas para
	 * a aplica„o desejada.
	 * 
	 * A API/ESRI-java-geometry È utilizada para o geoprocessamento
	 * a partir de operadores. e.g. OperatorTouches.local().execute(...)
	 * foram feitos funÁıes de teste para dados de arquivo de entrada
	 *	111.133km~1∫
	 */
	/**********************************************************************/
			//exemplos

	//calcular todo o comprimento das geometrias com esse nome
	public String comprimento_total(String nome, String path){
		
		int g=0;
		int h=0;
		double comprimento=0.0;
		String geojson;
		List<Geo> resultado=new ArrayList<Geo>();
		try {
			resultado=BancoDados.BuscarGeo(2,nome);
		}catch (ClassNotFoundException e) {
			System.out.println(e.getMessage());
			e.printStackTrace();
		}	

		for(int i=0;i<resultado.size();i++){
			g+=1;
			if (resultado.get(i).getTipo().equals("Polyline")){
				comprimento+=Comprimento(resultado.get(i));
				h+=1;
			}	
		}
		
		System.out.println("comprimento total :"+h+"/"+g+":  "+String.valueOf(comprimento)+"->"+String.valueOf(comprimento*111.133)+" kms");
		geojson=Geometry2GeoJson(resultado,path);
	
		return geojson;
	}
	
	//localiza todas geometrias com esa propriedade dentro de um raio
	public List<Geo>  localizar_raio(double latitude, double longitude, int raio, String nome, String path){
		
		int g=0;
		int h=0;
		double comprimento=0.0;
		Geo geo= new Geo();
		geo.setLatitude(latitude);
		geo.setLongitude(longitude);
		geo.setGeometria(new Point(latitude,longitude));
		double dist;
		List<Geo> resultado=new ArrayList<Geo>();
		try {
			resultado=BancoDados.BuscarGeo(3,nome);
		}catch (ClassNotFoundException e) {
			System.out.println(e.getMessage());
			e.printStackTrace();
		}	

		
				
		for(int i=0;i<resultado.size();i++){
			g+=1;
			dist=Distancia(resultado.get(i),geo)*1000;//para metro
			System.out.println(String.valueOf(dist));
			if (dist* 111.133>raio){
				resultado.remove(i);
				i-=1;
				h+=1;
			}	
		}
		System.out.print("comparacoes total :"+g);
		System.out.print(" comparacoes fora :"+h);
		System.out.println(" comparacoes dentro :"+String.valueOf(g-h));
		
		Geometry2GeoJson(resultado,path);
	
		return resultado;
	}
	
	//busca uma geometria com o nome
	//busca geometrias contidas
	public List<Geo> dentro (String nome, String path){
		
		List<Geo> geo=new ArrayList<Geo>();
		List<Geo> resultado=new ArrayList<Geo>();
		String geojson;
		
		int g=0;
		int h=0;
		
		try {
			geo=BancoDados.BuscarGeo(1,nome);
			resultado=BancoDados.BuscarGeo(0,null);
		}catch (ClassNotFoundException e) {
			System.out.println(e.getMessage());
			e.printStackTrace();
		}	
		
		if(geo.size()!=1){
			System.out.println("erro");
		}
		
		for(int i=0;i<resultado.size();i++){
			g+=1;
			if(!Contem(geo.get(0).getGeometria(),resultado.get(i).getGeometria()) || resultado.get(i).getId().equals(nome)){
				resultado.remove(i);
				i-=1;
				h+=1;
			}
		}
		System.out.print("comparacoes total :"+g);
		System.out.print(" comparacoes fora :"+h);
		System.out.println(" comparacoes dentro :"+String.valueOf(g-h));
		return resultado;
	}
	
	public double Distancia(Geo g1, Geo g2){

		/*double theta = g1.getLongitude() - g2.getLongitude();
        double distance = Math.sin(Math.toRadians(g1.getLatitude())) * Math.sin(Math.toRadians(g2.getLatitude())) +
                       Math.cos(Math.toRadians(g1.getLatitude())) * Math.cos(Math.toRadians(g2.getLatitude())) *
                       Math.cos(Math.toRadians(theta));

        distance = Math.acos(distance);
        distance = Math.toDegrees(distance);
        distance = distance * 111.133;

        return distance;
        */
		
		ProgressTracker pt = null;//= new ProgressTracker();
		SpatialReference.create(4326);
		double dist=OperatorDistance.local().execute(g1.getGeometria(), g2.getGeometria(), pt);
		return dist;
	}
	
	public double Comprimento(Geo geo){
		if (geo!=null){
			return geo.getGeometria().calculateLength2D();
		}
		else return 0.0;
	}
	
	//
	public String porcentagem_area(String nome, String path) throws ClassNotFoundException, NullPointerException{
		String geojson;
		List<Geo> lista= dentro(nome, path);
		List<Geo> id=BancoDados.BuscarGeo(2, nome);
		double a=area_total(lista);
		double at=area_total(id);
	
		System.out.println("area1:"+ String.valueOf(a));
		System.out.println("area2:"+ String.valueOf(at));
		System.out.println("area1/area2:"+ String.valueOf(a/at));
		geojson=Geometry2GeoJson(lista,path);
	return geojson;
		
}
	
	public double area_total(List<Geo> lista){
		
		double area=0.0;
		for(int i=0; i<lista.size();i++){
			area+=lista.get(i).getGeometria().calculateArea2D();
		}
		area=area*111.133*111.133;
		return area;
	}
	
	
	
/*****************************************************/
	//teste da API ESRI-geometry-java
	
	
 	public  void InicializarTestes(String path){
		
			BancoDados.LimparBanco();
			List<Geo> geo;
			try {
				geo = GeoJson2Geometry(path);
				ImportarBanco(geo);
			} catch (IOException e) {
				e.printStackTrace();
			} catch (JSONException e) {
				e.printStackTrace();
			}
	
	}
	
	public String testeContain(){
		
		List<Geo> lista_geo = null;
		List<Geo> lista = new ArrayList<Geo>();
		try {
			lista_geo = ExportarBanco();
		} catch (ClassNotFoundException e1) {
			e1.printStackTrace();
		} catch (NullPointerException e1) {
			e1.printStackTrace();
		}
		if(lista_geo.isEmpty()){
			System.out.println("banco vazio");
			return "banco vazio";
		}
		System.out.println("geometrias estao contidas:");
		for(int i=0;i<lista_geo.size()-1;i++){
			Geo geo=lista_geo.get(i);
			
			for(int j=i+1;j<lista_geo.size();j++){
				Geo geo2=lista_geo.get(j);
				if (Contem(geo.getGeometria(),geo2.getGeometria())){
					System.out.println(geo.getId()+" "+geo2.getId());
					lista.add(geo);
					//j=lista_geo.size();
				}
				if (Contem(geo2.getGeometria(),geo.getGeometria())){
					System.out.println(geo2.getId()+" "+geo.getId());
					lista.add(geo2);
					//j=lista_geo.size();
				}
			}
			
		}
	
		return Geometry2GeoJson(lista,null);
	}
	
	public String testeCrosses(){
		
		List<Geo> lista_geo = null;
		List<Geo> lista = new ArrayList<Geo>();
		try {
			lista_geo = ExportarBanco();
		} catch (ClassNotFoundException e1) {
			e1.printStackTrace();
		} catch (NullPointerException e1) {
			e1.printStackTrace();
		}
		if(lista_geo.isEmpty()){
			System.out.println("banco vazio");
			return "banco vazio";
		}
		System.out.println("geometrias se cruzam:");
		for(int i=0;i<lista_geo.size()-1;i++){
			Geo geo=lista_geo.get(i);
			
			for(int j=i+1;j<lista_geo.size();j++){
				Geo geo2=lista_geo.get(j);
				if (Cruza(geo.getGeometria(),geo2.getGeometria())){
					System.out.println(geo.getId()+" "+geo2.getId());
					lista.add(geo);
					lista.add(geo2);
		//			j=lista_geo.size();
				}
			}
			
		}
	
		return Geometry2GeoJson(lista,null);
	}
	
	public String testeDisjoint(){
		
		List<Geo> lista_geo = null;
		List<Geo> lista = new ArrayList<Geo>();
		try {
			lista_geo = ExportarBanco();
		} catch (ClassNotFoundException e1) {
			e1.printStackTrace();
		} catch (NullPointerException e1) {
			e1.printStackTrace();
		}
		if(lista_geo.isEmpty()){
			System.out.println("banco vazio");
			return "banco vazio";
		}
		System.out.println("geometrias disjuntas:");
		for(int i=0;i<lista_geo.size()-1;i++){
			Geo geo=lista_geo.get(i);
			
			for(int j=i+1;j<lista_geo.size();j++){
				Geo geo2=lista_geo.get(j);
				if (Disjunto(geo.getGeometria(),geo2.getGeometria())){
					System.out.println(geo.getId()+" "+geo2.getId());
					lista.add(geo);
					lista.add(geo2);
			//		j=lista_geo.size();
				}
			}
			
		}

		return Geometry2GeoJson(lista,null);
	}
	
	public String testeIntersect(){
		
		List<Geo> lista_geo = null;
		List<Geo> lista = new ArrayList<Geo>();
		try {
			lista_geo = ExportarBanco();
		} catch (ClassNotFoundException e1) {
			e1.printStackTrace();
		} catch (NullPointerException e1) {
			e1.printStackTrace();
		}
		if(lista_geo.isEmpty()){
			System.out.println("banco vazio");
			return "banco vazio";
		}
		System.out.println("geometrias se tocam:");
		for(int i=0;i<lista_geo.size()-1;i++){
			Geo geo=lista_geo.get(i);
			
			for(int j=i+1;j<lista_geo.size();j++){
				Geo geo2=lista_geo.get(j);
				if (Intersecta(geo.getGeometria(),geo2.getGeometria())){
					System.out.println(geo.getId()+" "+geo2.getId());
					lista.add(geo);
					lista.add(geo2);
				//	j=lista_geo.size();
				}
			}
			
		}

		return Geometry2GeoJson(lista,null);
	}

	public String testeOverlap(){
		
		List<Geo> lista_geo = null;
		List<Geo> lista = new ArrayList<Geo>();
		try {
			lista_geo = ExportarBanco();
		} catch (ClassNotFoundException e1) {
			e1.printStackTrace();
		} catch (NullPointerException e1) {
			e1.printStackTrace();
		}
		if(lista_geo.isEmpty()){
			System.out.println("banco vazio");
			return "banco vazio";
		}
		System.out.println("geometrias se sobrepoe:");
		for(int i=0;i<lista_geo.size()-1;i++){
			Geo geo=lista_geo.get(i);
			
			for(int j=i+1;j<lista_geo.size();j++){
				Geo geo2=lista_geo.get(j);
				if (Sobrepoe(geo.getGeometria(),geo2.getGeometria())){
					System.out.println(geo.getId()+" "+geo2.getId());
					lista.add(geo);
					lista.add(geo2);
					//j=lista_geo.size();
				}
			}
			
		}
	
		return Geometry2GeoJson(lista,null);
	}
	
	public  String testeTouch(){

		List<Geo> lista_geo = null;
		List<Geo> lista = new ArrayList<Geo>();
		try {
			lista_geo = ExportarBanco();
		} catch (ClassNotFoundException e1) {
			e1.printStackTrace();
		} catch (NullPointerException e1) {
			e1.printStackTrace();
		}
		if(lista_geo.isEmpty()){
			System.out.println("banco vazio");
			return "banco vazio";
		}
		System.out.println("geometrias se tocam:");
		for(int i=0;i<lista_geo.size()-1;i++){
			Geo geo=lista_geo.get(i);
			
			for(int j=i+1;j<lista_geo.size();j++){
				Geo geo2=lista_geo.get(j);
				if (Toca(geo.getGeometria(),geo2.getGeometria())){
					System.out.println(geo.getId()+" "+geo2.getId());
					lista.add(geo);
					lista.add(geo2);
				}
			}
			
		}
	
		return Geometry2GeoJson(lista,null);
	}
	
	public String testeWithin(){
		
		List<Geo> lista_geo = null;
		List<Geo> lista = new ArrayList<Geo>();
		try {
			lista_geo = ExportarBanco();
		} catch (ClassNotFoundException e1) {
			e1.printStackTrace();
		} catch (NullPointerException e1) {
			e1.printStackTrace();
		}
		if(lista_geo.isEmpty()){
			System.out.println("banco vazio");
			return "banco vazio";
		}
		System.out.println("geometrias dentro");
		for(int i=0;i<lista_geo.size()-1;i++){
			Geo geo=lista_geo.get(i);
			
			for(int j=i+1;j<lista_geo.size();j++){
				Geo geo2=lista_geo.get(j);
				if (Dentro(geo.getGeometria(),geo2.getGeometria())){
					System.out.println(geo.getId()+" "+geo2.getId());
					lista.add(geo);
					//j=lista_geo.size();
				}
				if (Dentro(geo2.getGeometria(),geo.getGeometria())){
					System.out.println(geo2.getId()+" "+geo.getId());
					lista.add(geo2);
					//j=lista_geo.size();
				}
			}
			
		}
	
		return Geometry2GeoJson(lista,null);
	}
	



	
	

	
	public static void MostrarPontos(Polygon c){
		Point2D[] p2d;
		Point2D p2;
		int x;
		p2d=c.getCoordinates2D();
		System.out.println("p2d.length: "+String.valueOf(p2d.length));
		for(x=0;x<p2d.length;x++){	
			p2=p2d[x];
			System.out.println("p2: "+String.valueOf(p2.x)+" "+String.valueOf(p2.y));
		}System.out.println();
		}
	
	public static void MostrarAreaDistancia(Geometry c){
		System.out.println("distancia: "+String.valueOf(c.calculateLength2D()) );
		System.out.println("area: "+String.valueOf(c.calculateArea2D()) );
		System.out.println();
		}
	
	
	
	
	/****************************************************************************************/
	
	//////////basico
	private static boolean Toca(Geometry a, Geometry b){
		SpatialReference sr = null;//= new SpatialReference();
		ProgressTracker pt = null;//= new ProgressTracker();
		SpatialReference.create(4326);
		boolean toca=false;
				
		if(a!=null && b!=null)
			toca = OperatorTouches.local().execute(a,b,sr,pt);
		else
			System.out.println("uma das geometrias √© nula");
		
		if(toca){
			System.out.println("as geometrias se tocam");
			return true;
		}else{System.out.println("as geometrias NO se tocam");}
			return false;
	}
	
	private static boolean Dentro(Geometry a, Geometry b){
		//a dentro de b
		
		SpatialReference sr = null;//= new SpatialReference();
		ProgressTracker pt = null;//= new ProgressTracker();
		SpatialReference.create(4326);
		boolean dentro=false;
		
		if(a!=null && b!=null)
			dentro = com.esri.core.geometry.OperatorWithin.local().execute(a, b, sr, pt);
		else
			System.out.println("uma das geometrias È nula");
		
		if(dentro){
			System.out.println(" 1∫ ---> 2∫");
			return true;
		}else{System.out.println(" 1∫ -/-> 2∫");
		}
			return false;
	}
			
	private static boolean Cruza(Geometry a, Geometry b){
		SpatialReference sr = null;//= new SpatialReference();
		ProgressTracker pt = null;//= new ProgressTracker();
		SpatialReference.create(4326);
		boolean cruza=false;
				
		if(a!=null && b!=null)
			cruza = OperatorCrosses.local().execute(a,b,sr,pt);
		else
			System.out.println("uma das geometrias √© nula");
		
		if(cruza){
			System.out.println("as geometrias se cruzam");
			return true;
		}else{System.out.println("as geometrias N√ÉO se cruzam");}
			return false;

	}
	
	private static boolean Intersecta(Geometry a, Geometry b){
		SpatialReference sr = null;//= new SpatialReference();
		ProgressTracker pt = null;//= new ProgressTracker();
		SpatialReference.create(4326);
		boolean intersecta=false;
				
		if(a!=null && b!=null)
			intersecta = OperatorIntersects.local().execute(a,b,sr,pt);
		else
			System.out.println("uma das geometrias √© nula");
		
		if(intersecta){
			System.out.println("as geometrias se intersectam");
			return true;
		}else{System.out.println("as geometrias N√ÉO se intersectam");}
			return false;

	}
	
	private static boolean Disjunto(Geometry a, Geometry b){
		SpatialReference sr = null;//= new SpatialReference();
		ProgressTracker pt = null;//= new ProgressTracker();
		SpatialReference.create(4326);
		boolean disjunto=false;
				
		if(a!=null && b!=null)
			disjunto = OperatorDisjoint.local().execute(a,b,sr,pt);
		else
			System.out.println("uma das geometrias √© nula");
		
		if(disjunto){
			System.out.println("as geometrias disjuntas");
			return true;
		}else{System.out.println("as geometrias juntas");}
			return false;

	}
	
	private static boolean Sobrepoe(Geometry a, Geometry b){
		SpatialReference sr = null;//= new SpatialReference();
		ProgressTracker pt = null;//= new ProgressTracker();
		SpatialReference.create(4326);
		boolean sobrepoe=false;
				
		if(a!=null && b!=null)
			sobrepoe = OperatorOverlaps.local().execute(a,b,sr,pt);
		else
			System.out.println("uma das geometrias √© nula");
		
		if(sobrepoe){
			System.out.println("as geometrias se sobrepoe");
			return true;
		}else{System.out.println("as geometrias N√ÉO se sobrepoe");}
			return false;

	}
	
	private static boolean Contem(Geometry a, Geometry b){
		SpatialReference sr = null;//= new SpatialReference();
		ProgressTracker pt = null;//= new ProgressTracker();
		SpatialReference.create(4326);
		boolean contem=false;
				
		if(a!=null && b!=null)
			contem = OperatorContains.local().execute(a,b,sr,pt);
		else
			System.out.println("uma das geometrias √© nula");
			
		if(contem){
			//A Contem B
			System.out.println("a geometria est√° contida");
			return true;
		}else{System.out.println("a geometria N√ÉO  n√£o est√° contida");}
			return false;

	}
	/////////

	//OK
	//recebe um arquivo com caminho completo do tipo geojson
	//ou recebe uma string contendo o geojson
	//adiciona todos as geometrias no banco
	public  List<Geo>  GeoJson2Geometry(String arquivo) throws IOException, JSONException{
	
	String id=null;
	String type=null;
	String coordinates=null;
	Propriedade<String> properties= new Propriedade<String>();
	
	
	String geojson="";
	geo = null;
	Geometry geometry;
	List<Geo> lista_geo=new ArrayList<Geo>();

	BufferedReader buffer = null;
	//(String arquivo) pode ser a string geojson e si ou o caminho para o arquivo
	if(new File(arquivo+".geo.json").exists() || new File(arquivo+".geojson").exists()||new File(arquivo).exists()){
		
		if(new File(arquivo+".geojson").exists())
		buffer = new BufferedReader(new InputStreamReader(new FileInputStream(arquivo+".geojson"), "UTF-8"));
		// buffer=new BufferedReader(new FileReader(arquivo+".geojson"));
		else if(new File(arquivo+".geo.json").exists())
			buffer = new BufferedReader(new InputStreamReader(new FileInputStream(arquivo+".geo.json"), "UTF-8"));
		else
			buffer = new BufferedReader(new InputStreamReader(new FileInputStream(arquivo), "UTF-8"));
		
		
		while(buffer.ready()){
			geojson+=buffer.readLine();
		}
		buffer.close();
	}else 
		geojson=arquivo;
	
	if(geojson==null)
		throw new NullPointerException(); 
	
	geojson = geojson.replaceAll( "\t", "" );
	geojson = geojson.replaceAll( "\n", "" );
	geojson = geojson.replaceAll( "\r", "" );
	geojson = geojson.replaceAll( "\f", "" );
	String[] geostrings=geojson.split("\\},\\{");
	
	//i=0=> {"type":"*Feature*Collection
	//i=1 => Collection","features":[\t{\t"type": "*Feature*
	for(int i=0; i<geostrings.length;i++){
		
		id=getId(geostrings[i]);
		type=getType(geostrings[i]);
		properties=getProperties(geostrings[i]);
		coordinates=getCoordinates(geostrings[i]);
		
		if(id==null){//pega outro valor pra id e.g. "name", "nome", propriedades[2],propriedades[1]...
			
			if(properties.tamanho()==0){
				//throw new NullPointerException("geometria sem id e sem propiedades");
				//continue;//geometrias sem id e propriedade ser√£o ignoradas
				id="noid";
				properties.put("properties", "null");
			}
			else if(properties.tamanho()==1){
				
				switch(properties.getV()){
					case "yes":
						id=properties.getP();
						break;
					default : 
						id=properties.getV();
						break;
				}
			}
			else{
				for(int j=0;j<properties.tamanho();j++){
					if (properties.getP(j).equals("name")){id=properties.getV(j); j=properties.tamanho();}
					else if(properties.getP(j).equals("nome")){id=properties.getV(j); j=properties.tamanho();}
					else if(properties.getP(j).equals("highway")){id=properties.getP(j); j=properties.tamanho();}
				}
				
				if(id==null){id=properties.getV();} //pega primeira propriedade
			}	
		}
		
		
		//System.out.println("----------------IMPORT GEOJSON----------------\n"+geostrings[i]+"\n"+i+"\n"+id+"\n"+type+"\n\n");
		if(coordinates==null){
			System.err.println("Erro ao importar geojson: Coordenadas mau definidas.");
			return null;
		}else if(id==null){
			System.err.println("Erro ao importar geojson: id mau definido.");
			return null;
		}else if(type == null){
			System.err.println("Erro ao importar geojson: tipo mau definido.");
			return null;
		}else{
			ProgressTracker pt = null;
			geo=new Geo();
			switch(type){
				case "Point":
					geo.setGeometria((Point) OperatorImportFromGeoJson.local().execute
						(com.esri.core.geometry.GeoJsonImportFlags.geoJsonImportDefaults,
								Geometry.Type.Point, coordinates, pt).getGeometry());
					break;
				case "LineString":
					Polyline ls= (Polyline) OperatorImportFromGeoJson.local().execute
						(com.esri.core.geometry.GeoJsonImportFlags.geoJsonImportDefaults,
								Geometry.Type.Polyline, coordinates, pt).getGeometry();
					/*if(ls.getPathCount()>1){
						System.err.println("geojson possui geometrias desconhecidas...");
						return null;
					}*/
					geo.setGeometria(ls);
					break;
				case "Polygon":
					geometry=OperatorImportFromGeoJson.local().execute
						(com.esri.core.geometry.GeoJsonImportFlags.geoJsonImportDefaults,
								Geometry.Type.Polygon, coordinates, pt).getGeometry();
					Polygon pg= (Polygon)geometry;
					/*if(pg.getPathCount()>1){
						System.err.println("geojson possui geometrias desconhecidas...");
						return null;
					}*/
					geo.setGeometria(pg);
					break;
				case "MultiPoint":
					geo.setGeometria((MultiPoint)OperatorImportFromGeoJson.local().execute
							(com.esri.core.geometry.GeoJsonImportFlags.geoJsonImportDefaults,
									Geometry.Type.MultiPoint, coordinates, pt).getGeometry());
					break;
				case "MultiLineString":
					Polyline mls= (Polyline)OperatorImportFromGeoJson.local().execute
						(com.esri.core.geometry.GeoJsonImportFlags.geoJsonImportDefaults,
								Geometry.Type.Polyline, coordinates, pt).getGeometry();
					
					/*if(mls.getPathCount()<2){
						System.err.println("geojson possui geometrias desconhecidas...");
						return null;
					}*/
					geo.setGeometria(mls);
					break;
				case "MultiPolygon": 
					Polygon mpg= (Polygon)OperatorImportFromGeoJson.local().execute
						(com.esri.core.geometry.GeoJsonImportFlags.geoJsonImportDefaults,
								Geometry.Type.Polygon, coordinates, pt).getGeometry();
					/*if(mpg.getPathCount()<2){
							System.err.println("geojson possui geometrias desconhecidas...");
							return;
					}*/
					geo.setGeometria(mpg);
					break;
				default:
					System.err.println("geojson possui geometrias desconhecidas..."+ type);
					return null;
			}
		
		}
		
		
		
		
		geo.setTipo(type);
		geo.setPropriedades(properties);
		geo.setId(id);
		lista_geo.add(geo);
		
		//if (properties!=null)
			//properties.esvaziar();
	}
	return lista_geo;
}

	
	//OK
	//recebe uma lista de geometrias e exporta para geojson como arquivo dentra da pasta especificada
	//ou retorna como string
	public String Geometry2GeoJson(List<Geo> lista,String path){
		String geojson="{\"type\":\"FeatureCollection\",\"features\":[\n";
			List geometrias = new ArrayList();
			String geometria = null;
			int i=0;
			Propriedade<String> prop;
			//cada consulta adiciona um uma geometria ao geojson
			while (!lista.isEmpty()){
				//tipo
				//tipo coordendas
				if(lista.get(0).getGeometria()!=null){
					if(lista.get(0).getTipo().equals("Line")){
						//Line n√£o √© um tipo geojson
						Line l=(Line) lista.get(0).getGeometria();
						Polyline pl= new Polyline();
						pl.startPath(l.getStartX(),l.getStartY());
						pl.lineTo(l.getEndX(), l.getEndY());
						lista.get(0).setTipo("Polyline");
						lista.get(0).setGeometria(pl);
					}
					
					/*if(lista.get(0).getTipo().equals("Polygon")){
						if (lista.get(0).getGeometria().isMultiVertex(0)){
							System.out.println("");
						}
					}*/
					
				/*	geojson+="\t{\t\"type\": \"Feature\",\n";
					geojson+="\t\t\"geometry\":";
					geojson+=OperatorExportToGeoJson.local().execute(lista.get(0).getGeometria());
					geojson+=",\n";
				*/
					geometria="{\"type\": \"Feature\",\"geometry\":";
					geometria+=OperatorExportToGeoJson.local().execute(lista.get(0).getGeometria())+",";
					
					//propriedades
					
					if(!lista.get(0).getPropriedades().vazio()){
						prop=lista.get(0).getPropriedades();
						geometria+= "\"properties\": {";//"tabela" de informa√ß√µes
						//geojson+= "\t\t\"properties\": {";//"tabela" de informa√ß√µes
						while(!prop.vazio()){
							geometria+="\""+prop.getP()+"\":\""+prop.getV()+"\"";
							prop.remove();//de cima
							if(!prop.vazio())
								geometria+=",";
						}
						geometria+="}}";
						//geojson+="}\n\t}";
					}else
						geometria+="}";
				
					lista.remove(0);
					
					if(!lista.isEmpty())
						geometria+=",";

				}else
					lista.remove(0);
				
				geometrias.add(geometria);
			}
			
			

			//salvar arquivo
			try {
				BufferedWriter bufferedWriter = null;
				if(path.substring(path.length()-4, path.length()).contains("json")){
						if (path!=null){
								 bufferedWriter = new BufferedWriter(
									new OutputStreamWriter(
								      new FileOutputStream(path), "utf-8"));
						}
				}else{
						if (path!=null){
								 bufferedWriter = new BufferedWriter(
									new OutputStreamWriter(
								      new FileOutputStream(path+"\\out.geojson"), "utf-8"));
						}
				}
				
				bufferedWriter.write(geojson);
				while(!geometrias.isEmpty()){
					bufferedWriter.write(geometrias.get(0).toString()+"\n");
					geometrias.remove(0);
				}
				bufferedWriter.write("]}");
				bufferedWriter.close();
				
			}catch (IOException e) {
				e.printStackTrace();
			}
			
			return geojson;
		}

	
	public void ImportarBanco(List<Geo> lista_geo){
		BancoDados.InserirGeo(lista_geo);
	}
		
	public List<Geo> ExportarBanco() throws ClassNotFoundException, NullPointerException{
		return BancoDados.BuscarGeo(0,null);
	}
		
	//public Geo BuscarBanco(String nome) throws ClassNotFoundException, NullPointerException{
		//return BancoDados.BuscarGeo(nome);
	//}
		
	private String getCoordinates(String geojson){
		int x,indice=0,inicio=-1,fim=0;
		 indice=geojson.indexOf("\"coordinates\"");
		 if(indice==-1)
			 return null;
		for(x=indice;x>=0;x--){
			if(geojson.charAt(x)=='{'){
				inicio=x;
				x=-1;
			}		
		}
		
		if(inicio<0)
			return null;
		
		for(x=indice;x<geojson.length();x++){
			if(geojson.charAt(x)=='}'){
				fim=x+1;
				x=geojson.length();
			}		
		}
		
		return geojson.substring(inicio, fim);
	}

	private String getType(String geojson){
		int x,indice=0,inicio=0,fim=0;
		 indice=geojson.indexOf("\"type\"");
		 if(indice==-1)
			 return null;
		for(x=indice;x<geojson.length();x++){
			if(geojson.charAt(x)==':'){
				inicio=x;
				x=geojson.length();
			}		
		}
		
		if(inicio==0)
			return null;
		
		for(x=inicio;x<geojson.length();x++){
			if(geojson.charAt(x)=='"'){
				inicio=x+1;
				x=geojson.length();
			}		
		}
		
		for(x=inicio+1;x<geojson.length();x++){
			if(geojson.charAt(x)=='"'){
				fim=x;
				x=geojson.length();
			}		
		}
		
		 String tipo=geojson.substring(inicio, fim);
		
		if(!tipo.equals("Point") && 
				!tipo.equals("LineString") && 
				!tipo.equals("Polygon") && 
				!tipo.equals("MultiPoint") && 
				!tipo.equals("MultiLineString") &&
				!tipo.equals("MultiPolygon"))
			return getType(geojson.substring(indice+4,geojson.length()));
		else
			return tipo;
	}

	String getId(String geojson){
		int x,indice=0,inicio=0,fim=0;
		 indice=geojson.indexOf("\"id\"");
		 if(indice==-1)
			 return null;
		 
		for(x=indice;x<geojson.length();x++){
			if(geojson.charAt(x)==':'){
				inicio=x;
				x=geojson.length();
			}		
		}
		
		if(inicio==0)
			return null;
		
		for(x=inicio;x<geojson.length();x++){
			if(geojson.charAt(x)=='"'){
				inicio=x+1;
				x=geojson.length();
			}		
		}
		
		for(x=inicio+1;x<geojson.length();x++){
			if(geojson.charAt(x)=='"'){
				fim=x;
				x=geojson.length();
			}		
		}
		try{
			return geojson.substring(inicio, fim);
			}
		catch(Exception e){
			e.printStackTrace();
			return null;
		}
	}

	Propriedade<String> getProperties(String geojson){
		Propriedade<String> prop = new Propriedade<String>();;
		String propriedade;
		int x,indice=0,inicio=-1,fim=0;
		 indice=geojson.indexOf("properties");
		 if(indice==-1)
			 return null;
		 for(x=indice;x<geojson.length();x++){
				if(geojson.charAt(x)=='{'){
					inicio=x;
					x=geojson.length();
				}		
			}
		if(inicio<0)
			return null;
		
		for(x=indice;x<geojson.length();x++){
			if(geojson.charAt(x)=='}'){
				fim=x+1;
				x=geojson.length();
			}		
			
		}
		
		String propriedades=geojson.substring(inicio, fim);
		propriedades=propriedades.replaceAll(",\"", "\",\"");
		propriedades=propriedades.replaceAll("\",\"", "\"\",\"\"");
		
		String[] lista_prop=propriedades.split("\"\",\"");
		//String[] lista_prop=propriedades.split("\",\"");
		
		for(int i=0; i<lista_prop.length;i++){
			
			 indice=lista_prop[i].indexOf("\":\"");
			 if(indice==-1)
				 continue;
			 for(x=0;x<lista_prop[i].length();x++){
					if(lista_prop[i].charAt(x)=='"'){
						inicio=x;
						x=lista_prop[i].length();
					}		
				}
			if(inicio<0)
				continue;
			
			/*for(x=inicio+1;x<lista_prop[i].length();x++){
				if(lista_prop[i].charAt(x)=='"'){
					fim=x+1;
					x=lista_prop[i].length();
				}		
				
			}*/
			propriedade=lista_prop[i].substring(inicio+1, indice);
			

			for(x=lista_prop[i].length()-1;x>=0;x--){
				if(lista_prop[i].charAt(x)=='"'){
					fim=x;
					x=-1;
				}		
			}
			if(fim<0)
				continue;
			/*for(x=fim-1;x>=0;x--){
				if(lista_prop[i].charAt(x)=='"'){
					inicio=x;
					x=-1;
				}		
			}*/
			
			try{
				prop.put(propriedade,lista_prop[i].substring(indice+3, fim));
			}
			catch(Exception e){
				e.printStackTrace();
			}
		}
			
		return prop;
	}

	
	public void SalvarGeoJson(String path,String geojson){
		
		FileOutputStream fos;
		
		try {
			fos = new FileOutputStream(path);
			fos.write(geojson.getBytes());
			fos.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	

}