package Cassandra.gis.db;


import com.esri.core.geometry.Geometry;

public class Geo {

	private String id;
	private String tipo;
	private double latitude,longitude;
	private Propriedade<String> propriedades;
	private Geometry geometria;
	public Geo() {
		setPropriedades(null);
		setTipo(null);
		setId(null);
		setLatitude(0.0);
		setLongitude(0.0);
	}
	public String getId() {
		return id;
	}
	public void setId(String id) {
		this.id = id;
	}
	public String getTipo() {
		return tipo;
	}
	public void setTipo(String tipo) {
		this.tipo = tipo;
	}
	public double getLatitude() {
		return latitude;
	}
	public void setLatitude(double latitude) {
		this.latitude = latitude;
	}
	public double getLongitude() {
		return longitude;
	}
	public void setLongitude(double longitude) {
		this.longitude = longitude;
	}
	public Geometry getGeometria() {
		return geometria;
	}
	public void setGeometria(Geometry geometria) {
		this.geometria = geometria;
	}
	public Propriedade<String> getPropriedades() {
		return propriedades;
	}
	public void setPropriedades(Propriedade<String> propriedades) {
		this.propriedades = propriedades;
	}
}
