package Cassandra.gis.db;

import java.util.ArrayList;
import java.util.List;

public class Tupla<T1, T2> {

	private T1 p;
	private T2 v;
	private List<T1> Lp;
	private List<T2> Lv;
	public Tupla() {
		Lp= new ArrayList<T1>();
		Lv= new ArrayList<T2>();
		p=null;
		v=null;
	}
	public Tupla(int x) {}
	  // public void setP(propriedade t) { this.p = t; }
	  // public void setV(valor t) { this.v = t; }
	   public T1 getP() { return p; }
	   public T2 getV() { return v; }
	   
	   public T1 getP(int index) { 
		   return Lp.get(index);
	   }
	   public T2 getV(int index) { 
		   return Lv.get(index); 
		   }
	    
	    public void put(T1 Propriedade, T2 Valor) throws NullPointerException{
	    
	    	if(Propriedade==null || Valor==null){
	    		throw new NullPointerException("propriedades nao foi criada");
	    	}
	    	Lp.add(Propriedade);
	    	Lv.add(Valor);
	    	if(this.p==null)
	    		this.p=Propriedade;
	    	if(this.v==null)
	    		this.v=Valor;
	    }
	    
	    public void remove(){
	    	
	    	Lp.remove(0);
	    	Lv.remove(0);
	    	if(!Lp.isEmpty())
	    		this.p=Lp.get(0);
	    	if(!Lv.isEmpty())
	    		this.v=Lv.get(0);
	    }
	    
	    public boolean vazio(){
	    	
	    	if(Lp.isEmpty() || Lv.isEmpty())
	    		return true;
	    	
	    	return false;
	    }

	    public int tamanho(){
	    	
	    	if(Lp.size()==Lv.size()){
	    		return Lp.size();
	    	}
	    	throw new NullPointerException("propriedades corrompida");
	    }
	    
	    
	    
	    public void esvaziar(){
	    	while(!vazio()){
	    		remove();
	    	}
	    }
	    //nen uso isso
	/*	public Tupla<T1,T2> copia() {
			// TODO Auto-generated method stub
			int x;
			Tupla<T1, T2> copia =new Tupla<T1, T2>(0);
			copia.p=this.p;
			copia.v=this.v;
			copia.Lp= new ArrayList<T1>();
			copia.Lv= new ArrayList<T2>();
			for(x=0;x<this.Lp.size();x++)
				copia.Lp.add(this.Lp.get(x));
			for(x=0;x<this.Lv.size();x++)
				copia.Lv.add(this.Lv.get(x));
			
			return copia;
		}*/
}
