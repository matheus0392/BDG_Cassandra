package Cassandra.gis.db;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.UDTValue;
import com.esri.core.geometry.Geometry;
import com.esri.core.geometry.Point;
import com.esri.core.geometry.Point2D;
import com.esri.core.geometry.Line;
import com.esri.core.geometry.Polygon;
import com.esri.core.geometry.Polyline;
import com.google.common.reflect.TypeToken;
import com.esri.core.geometry.MultiPoint;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;



/*
 * geometria list< frozen<  list< frozen<POINT> >>>
 * 				POINT(x FLOAT, y FLOAT)		: tipo definido pelo usuario
 * 						frozen<>			: utilizado para tipos definidos pelo usuario
 *						frozen<POINT> 		: tipo básico geométrico: deriva linha,LineString, poligono,etc
 * 		  frozen< list< frozen<POINT>>>		: listas de pontos define uma geometria (1 ou + pontos)
 *  list< frozen< list< frozen<POINT>>>>    : lista de geometrias(estritamente do mesmo tipo):multiponto, multilinha, multipoligono
 *
 * ponto:		[[(x2,y2)]]
 * Linha: 		[[(x1,y1),(x2,y2),(x3,y3)]]
 * Poligono: 	[[(x1,y1),(x2,y2),(x3,y3),(x4,y4),(x5,y5),(x6,y6)]] 4º ponto implicito
 *
 * multiponto:		[[(x1,y1)],[(x2,y2)],[(x3,y3)]]
 * multiLinha:		[[(x1,y1),(x2,y2),(x3,y3)],[(x4,y4),(x5,y5),(x6,y6)]]
 * multiPoligono: 	[[(x1,y1),(x2,y2),(x3,y3)],[(x4,y4),(x5,y5),(x6,y6)]] 4º ponto implicito
 *
 */
public class BDgeo {
	
	

	

	private Cluster cluster;
	private Session session;
	@SuppressWarnings("unused")
	private String url="127.0.0.1";
	private String keyspace="geometry";
	private String ks= "CREATE KEYSPACE if not exists "+ keyspace +" WITH REPLICATION = "
			+ "{ 'class' : 'SimpleStrategy', 'replication_factor' : 1 };";
	private String 	point="CREATE TYPE IF NOT EXISTS "+keyspace+".POINT(x DOUBLE, y DOUBLE);";
	//Q1:(LAT-LONG-ID-GEOM-TIPO-PROPRIEDADES)
	private String table1 ="CREATE TABLE IF NOT EXISTS "+keyspace+".geom_latlng"
					+"(latitude DOUBLE,"
					+ "longitude DOUBLE,"
					+ "nome text,"
					+ "geometria_tipo text,"
					+ "propriedades map<text,text>,"
					+ "geometria list< frozen<  list< frozen<POINT> >>>,"//"geometria list< frozen<  list< frozen<POINT> >>>,"
					+ "PRIMARY KEY((longitude,latitude),nome)) with "
					+ "CLUSTERING ORDER BY ( nome ASC); ";
	
	//Q2:(ID-LAT-LONG-GEOM-TIPO-PROPRIEDADES)
	private String table2 ="CREATE TABLE IF NOT EXISTS "+keyspace+".geom_label"
					+"(latitude DOUBLE,"
					+ "longitude DOUBLE,"
					+ "nome text,"
					+ "geometria_tipo text,"
					+ "propriedades map<text,text>,"
					+ "geometria list< frozen<  list< frozen<POINT> >>>,"
					+ "PRIMARY KEY(nome, latitude,longitude)) with "
					+ "CLUSTERING ORDER BY (latitude ASC, longitude ASC);";
	
	private String  idx1="CREATE INDEX  if not exists  prop_value_1 ON geom_label (propriedades)";
	private String  idx2="CREATE INDEX  if not exists porp_key_1 ON geom_label (KEYS(propriedades))";

	private String  idx3="CREATE INDEX  if not exists  prop_value_2 ON geom_latlng (propriedades)";
	private String  idx4="CREATE INDEX  if not exists  porp_key_2 ON geom_latlng (KEYS(propriedades))";
	
	public BDgeo(String url, String keyspace) {
		
		this.url=url;
		this.keyspace=keyspace;
		cluster = Cluster.builder().addContactPoint(url).build();
		session=cluster.newSession();
		session.execute(ks);
		session = cluster.connect(keyspace);
		session.execute(point);
		session.execute(table1);
		session.execute(table2);
		session.execute(idx1);
		session.execute(idx2);
		session.execute(idx3);
		session.execute(idx4);
	}
	
	public BDgeo(String[] url, String keyspace) {
		
		this.url=url[0];
		this.keyspace=keyspace;
		for(int i=0;i<url.length;i++)
			cluster = Cluster.builder().addContactPoint(url[i]).build();
		
		session=cluster.newSession();
		session.execute(ks);
		session = cluster.connect(keyspace);
		session.execute(point);
		//session.execute(table1);
		session.execute(table2);
	}
	

	
	//insere todas as geometrias(mesmo tipo) da lista no banco com as mesma chave (nome,lat,long)
	//public void InserirGeo(Geometry geom,String nome,Propriedade<String> propriedades) {
	public void InserirGeo(List<Geo> lista_geo) {
		if (lista_geo==null)
			return;
		
		Geo geo;
		
		
		while(!lista_geo.isEmpty()){
			geo=lista_geo.get(0);
			if (geo==null){
				System.out.println("não é possível adicionar geometrias ao banco: "
						+ "geometria nula");
				return;
			}
			if(geo.getGeometria().isEmpty()){
				System.out.println("não é possível adicionar geometrias ao banco: "
									+ "geometria vazia");
				return;
			}
			//cria string
			String table1= "INSERT INTO geom_latlng";
			String table2= "INSERT INTO geom_label";
			String query="(latitude, longitude, nome, geometria_tipo, propriedades, geometria) VALUES ";
			String _tipo="";
			String _geom = "";
			String   __=",";
			double lat = 0,lon=0;
			Point p = null;
			Line l= null;
			Polyline pl = null;
			Polygon pg=null;
			Point2D[] pp2d;
			List<Geometry> aux,aux2;
			
			
			/*------------------------------------------------------
			 * geometria list< frozen<  list< frozen<POINT> >>>
			 *------------------------------------------------------
			 * 				POINT(x FLOAT, y FLOAT)		: tipo definido pelo usuario
			 * 						frozen<>			: utilizado para tipos definidos pelo usuario
			 *						frozen<POINT> 		: tipo básico geométrico: ponto
			 * 		  frozen< list< frozen<POINT>>>		: listas de pontos define uma geometria (1 ou + pontos): linha,LineString(polyline(1)), poligono
			 *  list< frozen< list< frozen<POINT>>>>    : lista de geometrias(estritamente do mesmo tipo):multiponto, multilinha(polyline(n)), multipoligono
			 *
			 * === exemplo ===
			 * ponto:		[[(x1,y1)],[(x2,y2)],[(x3,y3)]]     3 pontos
			 * LinhaString: [[(x1,y1),(x2,y2)],(x3,y3)],[(x4,y4),(x5,y5)],(x6,y6)]]   2 curvas
			 * Poligono: 	[[(x1,y1),(x2,y2)],(x3,y3)],[(x4,y4),(x5,y5)],(x6,y6)]]   2 poligonos 4º ponto implicito
			 * ===============
			 */
			
			//while(!geo.getGeometria().isEmpty())
			
			//valores(lat, lon,tipo, geometria)//multiponto lat long é do primeiro ponto
			switch(geo.getGeometria().getType()){
			
				case MultiPoint:_tipo="'MultiPoint'"; 
					//criar lista<geometry> com cada ponto
					aux2 = new ArrayList<Geometry>();
	
						aux=pontos((MultiPoint)geo.getGeometria());
						while(!aux.isEmpty()){
							aux2.add(aux.get(0));
							aux.remove(0);
						}
						_geom+="[";
						while(!aux2.isEmpty()){
							p=(Point)aux2.get(0);  
							_geom+="[("+String.valueOf(p.getX())+__+String.valueOf(p.getY())+")]";
							aux2.remove(0);
							if(!aux2.isEmpty())
								_geom+=__;
						}
						_geom+="]";
						break;
						
				case Point:_tipo="'Point'";
						
							p=(Point)geo.getGeometria(); 
							lon=p.getX();
							lat=p.getY();
							_geom+="[[("+String.valueOf(p.getX())+__+String.valueOf(p.getY())+")]]";
							break;
			
				case Line: _tipo="'Line'";/*** acho que para geojson esse tipo eh obsoleto por causa da polilinha */
					
							l=(Line)geo.getGeometria();  
							lon=l.getStartX();
							lat=l.getStartY();
							_geom+="[[("+String.valueOf(lat)+__+String.valueOf(lon)+"),("+l.getEndX()+__+l.getStartY()+")]]";
							break;
	
				case Polyline:_tipo="'Polyline'";
				//cada polyline pode ter mais de um caminho
				//todos os caminhos de cada polyline é destrinchado e adicionado à lista
				//criar lista<geometry> com cada caminho de linha
							aux2 = new ArrayList<Geometry>();
							int i;
							
								aux=polilinhas((Polyline)geo.getGeometria());
								while(!aux.isEmpty()){
									aux2.add(aux.get(0));
									aux.remove(0);
								}
							
							
							pl = (Polyline)aux2.get(0);
							pp2d=pl.getCoordinates2D();
							//latitude e longitude
							lon=pp2d[0].x;
							lat=pp2d[0].y;
							//para cada, caminho adiciona as linhas correspondentes
							_geom+="[";
							while(!aux2.isEmpty()){
								pl = (Polyline)aux2.get(0);
								pp2d=pl.getCoordinates2D();
								
								if(pp2d.length<2){
									System.out.println("não é possível adicionar geometria ao banco: "
										 			+ "geometria POLYLINE corrompida");
									//return;
									continue;
								}
								
								
								_geom+="[";
								for(i=0;i<pp2d.length;i++){
									_geom+="("+String.valueOf(pp2d[i].x)+__+String.valueOf(pp2d[i].y)+")";
									if(i!=pp2d.length-1) _geom+=__;
									else _geom+="]";
								}
								if(pp2d[i-1].x==pp2d[0].x && pp2d[i-1].y==pp2d[0].y){
									System.err.println("não é possível adicionar geometria ao banco: "
													+ "Polilinha é um poligono");	
									//return;
									continue;
								 }
							
								 aux2.remove(0);
								 if(!aux2.isEmpty())
									_geom+=__;
							}
							_geom+="]";
							break;
									
				case Polygon: _tipo="'Polygon'";
				
							aux2 = new ArrayList<Geometry>();
							
							aux=poligonos((Polygon)geo.getGeometria());
							while(!aux.isEmpty()){
								aux2.add(aux.get(0));
								aux.remove(0);
							}
				
							pg = (Polygon)aux2.get(0);
	
							lon=pg.getXY(0).x;
							lat=pg.getXY(0).y;
							_geom+="[";
							while(!aux2.isEmpty()){
								pg = (Polygon)aux2.get(0);
								
								if(pg.getPointCount()<2){
									System.err.println("não é possível adicionar geometria ao banco: "
										 			+ "geometria POLYGON corrompida");
									//return;
									continue;
								}
								_geom+="[";
								//nao possui a repetição do primeiro ponto
								for(i=0;i<pg.getPointCount();i++){
								_geom+="("+String.valueOf(pg.getXY(i).x)+__+String.valueOf(pg.getXY(i).y)+")";
									if(i!=pg.getPointCount()-1) _geom+=__;
									else _geom+="]";
								}
								aux2.remove(0);
								 if(!aux2.isEmpty())
									_geom+=__;
							}
							_geom+="]";
							//nao possui a repetição do primeiro ponto
							//assumido que há uma linha entre o ultimo e o primeiro ponto
							
								
							break;
					default: 
						System.err.println("não é possível adicionar geometria ao banco: "
										+ "geometria com tipo indefinido");
					//return;
					continue;
			}
			//-----------------------------------------------------------
			//completa string
			
			if(geo.getId().contains("'")){
				geo.setId(geo.getId().replaceAll("'", "''"));
			}
			
			query+="("+String.valueOf(lat)+__+String.valueOf(lon)+__+"'"+geo.getId()+"'"+__+_tipo+__+"{";
			//propriedades e outros
			
			while(!geo.getPropriedades().vazio()){
				
				String propp=geo.getPropriedades().getP();
				String propv=geo.getPropriedades().getV();
				if(propp.contains("'")||propv.contains("'")){
					propp=propp.replaceAll("'", "''");
					propv=propv.replaceAll("'", "''");
				}
				query+="'"+propp+"':'"+propv+"'";
				geo.getPropriedades().remove();
				if(!geo.getPropriedades().vazio()) query+=__;
			}
			//fim
			query+="}"+__+_geom+");";
			//execute
			//try{
			//System.out.println("insert into tables ->"+query);
			session.execute(table1+query);
			session.execute(table2+query);
			//Thread.sleep(500);
			//}catch(Exception e){ System.err.println(e.getMessage());}
			//geo.setLatitude(lat);
			//geo.setLongitude(lon);

			lista_geo.remove(0);
		}
}
	
	//busca a geometria com chave primaria 'nome'
	/*public Geo BuscarGeo(String nome) throws ClassNotFoundException{
		// for (Row row : session.execute("SELECT * FROM table1"))
		//nome="minha quadra";
		
		List<Row> linha = session.execute("SELECT* from geom_label WHERE id='"+nome+"';").all();//latitude, longitude, nome, geometria, geometria_tipo, propriedades FROM geom_label "
		if(linha.size()!=1){
			System.out.println("nenhuma ou mais de uma geometria no banco para essa consulta");
			return null;
		}
	//-----------------------------------------------
		Geo geo= new Geo();	
		geo.setLatitude(linha.get(0).getDouble(1));
		geo.setLongitude(linha.get(0).getDouble(2));
		geo.setId(linha.get(0).getString(0));
		geo.setTipo(linha.get(0).getString(4));
		//---
		Propriedade<String> propriedades =new Propriedade<String> ();
		Map<?, ?> map = linha.get(0).getMap(5,Class.forName("java.lang.String"),Class.forName("java.lang.String"));
		Set<?> set = map.keySet();
	    Iterator<?> setIterator = set.iterator();
	     while(setIterator.hasNext()){
	    	 Object key = setIterator.next();
	         propriedades.put(key.toString(), map.get(key).toString());
	     }
	    //--- 
	     int cont;
	     List<UDTValue> registro_primario=null;
	     List<List<UDTValue>>  registro=linha.get(0).getList(3, new TypeToken<List<UDTValue>>(){
			private static final long serialVersionUID = 1L;});
	     
	     switch(geo.getTipo()){
	     	case "Point":
	     		for(int x=0;x< registro.size();x++){
	     			registro_primario=registro.get(x);
					if(registro_primario.size()!=1){
						System.err.println("diferente de um ponto para a geometria 'Ponto'");
						linha.remove(0);
						continue;
					}
					//
					geo.setGeometria(new Point(registro_primario.get(0).getDouble(0),registro_primario.get(0).getDouble(1)));
					//
					registro_primario.remove(0);
	     		}
	     		//acredito que todos os objetos foram removidos dentro do for(){while(){}}
	     		registro.clear();*
			    break;
			    
	     	case "MultiPoint":
	     		MultiPoint mp= new MultiPoint();
	     		for(int x=0;x< registro.size();x++){
	     			registro_primario=registro.get(x);
					if(registro_primario.size()!=1){
						System.err.println("diferente de um ponto para a geometria 'Ponto'");
						linha.remove(0);
						continue;
					}
					//
					while(!registro_primario.isEmpty()){
						mp.add(new Point(registro_primario.get(0).getDouble(0),registro_primario.get(0).getDouble(1)));
						registro_primario.remove(0);
					}
					geo.setGeometria(mp);
					//
	     		}
	     		break;
	     		
	     	case "Line":// acho que para geojson esse tipo eh obsoleto por causa da polilinha
	     		Line l= new Line();
	     		registro_primario=registro.get(0);
	     		 if(registro_primario.size()!=2){
						System.err.println("diferente de dois ponto para a geometria 'Linha'");
						linha.remove(0);
						return null;
					}
		   
		    	for(cont=0;cont<registro_primario.size();cont++){
		    		//
					if(cont==0)
						l.setStart(new Point(registro_primario.get(0).getDouble(0),registro_primario.get(0).getDouble(1)));
					else
						l.setEnd(new Point(registro_primario.get(0).getDouble(0),registro_primario.get(0).getDouble(1)));
					registro_primario.remove(cont);
		    	}
		    	geo.setGeometria(l);
			    break;
			    
	     	case "Polyline":
	     		Polyline pl= new Polyline();
	     		for(int geometrias=0;geometrias< registro.size();geometrias++){
		     		registro_primario=registro.get(geometrias);
		     		 if(registro_primario.size()<2){
							System.err.println("diferente de dois ponto para a geometria 'Linha'");
							linha.remove(0);
							continue;
						}
		     		 //para cada caminho pegar as linhas
			    	for(cont=0;cont<registro_primario.size();cont++){
			    		//
						if(cont==0)
							pl.startPath(new Point(registro_primario.get(cont).getDouble(0),registro_primario.get(cont).getDouble(1)));
						else
							pl.lineTo(new Point(registro_primario.get(cont).getDouble(0),registro_primario.get(cont).getDouble(1)));
			    	}
			    	for(cont=0;cont<registro_primario.size();){
			    		registro_primario.remove(0);
			    	}
	     		}
		    	geo.setGeometria(pl);
	     		break;
	     		
	     	case "Polygon":
	     		 Polygon pg= new Polygon();
	     		for(int geometrias=0;geometrias< registro.size();geometrias++){
		     		registro_primario=registro.get(geometrias);
		     		if(registro_primario.size()<3){
						System.err.println("menos de dois ponto para a geometria 'Poligono'");
						linha.remove(0);
						return null;
					}
		     		
		     		 //para cada caminho pegar as linhas
			    	for(cont=0;cont<registro_primario.size();cont++){
			    		//
						if(cont==0)
							pg.startPath(new Point(registro_primario.get(cont).getDouble(0),registro_primario.get(cont).getDouble(1)));
						else
							pg.lineTo(new Point(registro_primario.get(cont).getDouble(0),registro_primario.get(cont).getDouble(1)));
			    	}
			    	for(cont=0;cont<registro_primario.size();){
			    		registro_primario.remove(0);
			    	}
	     		}
		    	geo.setGeometria(pg);
	     		break;
	     		
	     	default:
	     		System.err.println("geometria nao definida, será ignorada");
				linha.remove(0);
				return null;
	     }
		
		geo.setPropriedades(propriedades);

		linha.remove(0);
		return geo;
}*/

	
	//busca todas as geometrias no banco
	public List<Geo> BuscarGeo(int select,String nome) throws ClassNotFoundException, NullPointerException{
			
			List<Row> linha = null;
			switch(select){
				case 0: linha= session.execute("SELECT * from geom_label;").all();
						break;
				case 1: linha = session.execute("SELECT * from geom_label WHERE nome='"+nome+"';").all();
						break;
				case 2: linha = session.execute("SELECT * from geom_label WHERE propriedades contains key '"+nome+"';").all();
						break;
				case 3: linha = session.execute("SELECT * from geom_label WHERE propriedades contains '"+nome+"';").all();
						break;
				case 4: linha= session.execute("SELECT * from geom_latlng;").all();	
						break;
				case 5: linha = session.execute("SELECT * from geom_latlng WHERE latitude=0.0 and longitude=0.0;").all();
						break;
				case 6: linha = session.execute("SELECT * from geom_latlng WHERE propriedades contains key '"+nome+"';").all();
						break;
				case 7: linha = session.execute("SELECT * from geom_latlng WHERE propriedades contains '"+nome+"';").all();
						break;
				default: linha= new ArrayList<Row>();break;
			}
	
				List<Geo> lista= new ArrayList<Geo>();
		//-----------------------------------------------	
			while( !linha.isEmpty()){
				Geo geo= new Geo();	
			if(select>3){//tabela geom_latlng
					geo.setLatitude(linha.get(0).getDouble(0));
					//---
					geo.setLongitude(linha.get(0).getDouble(1));
					//---
					geo.setId(linha.get(0).getString(2));
				}
			else{///tabela geom_label
					geo.setId(linha.get(0).getString(0));
					//---
					geo.setLatitude(linha.get(0).getDouble(1));
					//---
					geo.setLongitude(linha.get(0).getDouble(2));
				}
				//---
				geo.setTipo(linha.get(0).getString(4));
				
				//---
				Propriedade<String> propriedades =new Propriedade<String> ();
				Map<?, ?> map = linha.get(0).getMap(5,Class.forName("java.lang.String"),Class.forName("java.lang.String"));
				Set<?> set = map.keySet();
			    Iterator<?> setIterator = set.iterator();
			     while(setIterator.hasNext()){
			    	 Object key = setIterator.next();
			         propriedades.put(key.toString(), map.get(key).toString());
			     }
			    //--- 
			     //transformar em uma unica geometria
			     int cont;
			     List<UDTValue> registro_primario=null;
			     List<List<UDTValue>>  registro=linha.get(0).getList(3, new TypeToken<List<UDTValue>>(){
					private static final long serialVersionUID = 1L;});
			     
			     switch(geo.getTipo()){
			     	case "Point":
			     		for(int x=0;x< registro.size();x++){
			     			registro_primario=registro.get(x);
							if(registro_primario.size()!=1){
								System.err.println("diferente de um ponto para a geometria 'Ponto'");
								linha.remove(0);
								continue;
							}
							//
							geo.setGeometria(new Point(registro_primario.get(0).getDouble(0),registro_primario.get(0).getDouble(1)));
							//
							registro_primario.remove(0);
			     		}
			     		/*acredito que todos os objetos foram removidos dentro do for(){while(){}}
			     		registro.clear();*/
					    break;
					    
			     	case "MultiPoint":
			     		MultiPoint mp= new MultiPoint();
			     		for(int x=0;x< registro.size();x++){
			     			registro_primario=registro.get(x);
							if(registro_primario.size()!=1){
								System.err.println("diferente de um ponto para a geometria 'Ponto'");
								linha.remove(0);
								continue;
							}
							//
							while(!registro_primario.isEmpty()){
								mp.add(new Point(registro_primario.get(0).getDouble(0),registro_primario.get(0).getDouble(1)));
								registro_primario.remove(0);
							}
							geo.setGeometria(mp);
							//
			     		}
			     		break;
			     		
			     	case "Line":/*** acho que para geojson esse tipo eh obsoleto por causa da polilinha */
			     		Line l= new Line();
			     		registro_primario=registro.get(0);
			     		 if(registro_primario.size()!=2){
								System.err.println("diferente de dois ponto para a geometria 'Linha'");
								linha.remove(0);
								continue;
							}
				   
				    	for(cont=0;cont<registro_primario.size();cont++){
				    		//
							if(cont==0)
								l.setStart(new Point(registro_primario.get(0).getDouble(0),registro_primario.get(0).getDouble(1)));
							else
								l.setEnd(new Point(registro_primario.get(0).getDouble(0),registro_primario.get(0).getDouble(1)));
							registro_primario.remove(cont);
				    	}
				    	geo.setGeometria(l);
					    break;
					    
			     	case "Polyline":
			     		Polyline pl= new Polyline();
			     		for(int geometrias=0;geometrias< registro.size();geometrias++){
				     		registro_primario=registro.get(geometrias);
				     		 if(registro_primario.size()<2){
									System.err.println("diferente de dois ponto para a geometria 'Linha'");
									linha.remove(0);
									continue;
								}
				     		 //para cada caminho pegar as linhas
					    	for(cont=0;cont<registro_primario.size();cont++){
					    		//
								if(cont==0)
									pl.startPath(new Point(registro_primario.get(cont).getDouble(0),registro_primario.get(cont).getDouble(1)));
								else
									pl.lineTo(new Point(registro_primario.get(cont).getDouble(0),registro_primario.get(cont).getDouble(1)));
					    	}
					    	for(cont=0;cont<registro_primario.size();){
					    		registro_primario.remove(0);
					    	}
			     		}
				    	geo.setGeometria(pl);
			     		break;
			     		
			     	case "Polygon":
			     		 Polygon pg= new Polygon();
			     		for(int geometrias=0;geometrias< registro.size();geometrias++){
				     		registro_primario=registro.get(geometrias);
				     		if(registro_primario.size()<3){
								System.err.println("menos de dois ponto para a geometria 'Poligono'");
								linha.remove(0);
								return null;
							}
				     		
				     		 //para cada caminho pegar as linhas
					    	for(cont=0;cont<registro_primario.size();cont++){
					    		//
								if(cont==0)
									pg.startPath(new Point(registro_primario.get(cont).getDouble(0),registro_primario.get(cont).getDouble(1)));
								else
									pg.lineTo(new Point(registro_primario.get(cont).getDouble(0),registro_primario.get(cont).getDouble(1)));
					    	}
					    	for(cont=0;cont<registro_primario.size();){
					    		registro_primario.remove(0);
					    	}
			     		}
				    	geo.setGeometria(pg);
			     		break;
			     		
			     	default:
			     		System.err.println("geometria nao definida, será ignorada");
						linha.remove(0);
						continue;
			     }
				//---
				geo.setPropriedades(propriedades);
				linha.remove(0);
				lista.add(geo);
				}
			return lista;
	}
	


	//Polygon(n)-> list<Polygon(1)>(n)
	private List<Geometry> poligonos(Polygon polygon) {
		if(polygon==null){
			System.err.println("poligono nula...");
			return null;
		}		
		
		
		List<Geometry> lista= new ArrayList<Geometry>();
		int x,y,inicio;
		if(polygon.getPathCount()==1){
			lista.add(polygon);
			return lista;
		}
		
		for(x=0;x<polygon.getPathCount();x++){
			Polygon pg=new Polygon();
			Point ptOut = new Point();
			inicio=polygon.getPathStart(x);
			 for(y=inicio;y<polygon.getPathEnd(x);y++){
				 polygon.getPoint(y, ptOut);
				 if(y==inicio)
					pg.startPath(ptOut);
				 else
					 pg.lineTo(ptOut);
			 }
			 lista.add(pg);
		}
		return lista;
	}

	//Polyline(n)-> list<Polyline(1)>(n)
	private List<Geometry> polilinhas(Polyline polyline) {
		if(polyline==null){
			System.err.println("polylinha nula...");
			return null;
		}		
		
		
		List<Geometry> lista= new ArrayList<Geometry>();
		int x,y,inicio;
		
		if(polyline.getPathCount()==1){
			lista.add(polyline);
			return lista;
		}
		
		for(x=0;x<polyline.getPathCount();x++){
			Polyline pg=new Polyline();
			Point ptOut = new Point();
			inicio=polyline.getPathStart(x);
			 for(y=inicio;y<polyline.getPathEnd(x);y++){
				 polyline.getPoint(y, ptOut);
				 if(y==inicio)
					pg.startPath(ptOut);
				 else
					 pg.lineTo(ptOut);
			 }
			 lista.add(pg);
		}
		return lista;
	}
	
	//Multipoints(n)-> list<Points>(n)
	private List<Geometry> pontos(MultiPoint multipoints) {

			List<Geometry> lista= new ArrayList<Geometry>();
			for(int x=0;x<multipoints.getPointCount();x++){
				Point p=new Point();
				p=multipoints.getPoint(x);
				 lista.add(p);
			}
			multipoints.setEmpty();
			return lista;
		}
		

	public void LimparBanco(){
		
		session=cluster.newSession();
		session = cluster.connect(keyspace);
		session.execute("drop table if exists geom_label;");
		session.execute("drop table if exists geom_latlng;");
		session.execute(table1);
		session.execute(table2);
		session.execute(idx1);
		session.execute(idx2);
		session.execute(idx3);
		session.execute(idx4);

	}
	
}
