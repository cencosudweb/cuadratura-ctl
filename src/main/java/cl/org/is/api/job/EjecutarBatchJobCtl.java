package cl.org.is.api.job;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import com.ibm.as400.access.AS400;
import com.ibm.as400.access.AS400JDBCDriver;


public class EjecutarBatchJobCtl {

	private static BufferedWriter bw;
	private static String path;
	
	private static final int DIFF_HOY_FECHA_INI = 1;
	private static final int DIFF_HOY_FECHA_FIN = 0;

	/**
	 * 
	 * @param args
	 */
	public static void main(String[] args) {

		Map <String, String> mapArguments = new HashMap<String, String>();
		String sKeyAux = null;

		for (int i = 0; i < args.length; i++) {

			if (i % 2 == 0) {

				sKeyAux = args[i];
			}
			else {

				mapArguments.put(sKeyAux, args[i]);
			}
		}

		try {

			File info              = null;
			File miDir             = new File(".");
			path                   =  miDir.getCanonicalPath();
			info                   = new File(path+"/info.txt");
			bw = new BufferedWriter(new FileWriter(info));
			info("El programa se esta ejecutando...");
			crearTxt(mapArguments);
			System.out.println("El programa finalizo.");
			info("El programa finalizo.");
			bw.close();
		}
		catch (Exception e) {

			System.out.println(e.getMessage());
		}
	}

	private static void crearTxt(Map <String, String> mapArguments) {

		Connection dbconnection = crearConexion();
		Connection dbconnOracle = crearConexionOracle();
		File file1              = null;
		BufferedWriter bw       = null;
		BufferedWriter bw2      = null;
		PreparedStatement pstmt = null;
		StringBuffer sb         = null;
		String iFechaIni           = null;
		String iFechaFin           = null;
		
		String iFechaIni2           = null;
		String iFechaFin2           = null;
		
		String iFechaIni3           = null;
		String iFechaFin3           = null;

		try {

			try {
				
				iFechaIni = restarDias(mapArguments.get("-fi"), DIFF_HOY_FECHA_INI);
				iFechaFin = restarDias(mapArguments.get("-fi"), DIFF_HOY_FECHA_FIN);
				
				iFechaIni2 = restarDias2(mapArguments.get("-fi"), DIFF_HOY_FECHA_INI);
				iFechaFin2 = restarDias2(mapArguments.get("-fi"), DIFF_HOY_FECHA_FIN);
				
				iFechaIni3 = restarDiasTxt(mapArguments.get("-fi"), DIFF_HOY_FECHA_INI);
				iFechaFin3 = restarDiasTxt(mapArguments.get("-fi"), DIFF_HOY_FECHA_FIN);
				
				info("[iFechaIni]"+iFechaIni);
				info("[iFechaFin]"+iFechaFin);
				info("[iFechaIni2]"+iFechaIni2);
				info("[iFechaFin2]"+iFechaFin2);
				info("[iFechaIni3]"+iFechaIni3);
				info("[iFechaFin3]"+iFechaFin3);
			}
			catch (Exception e) {

				e.printStackTrace();
			}
			file1           = new File(path + "/CuadraturaCtl-"  + iFechaIni3 + "_"+ iFechaFin3 + ".txt");
			sb = new StringBuffer();
			sb.append("select a.LOAD_NBR, a.WHSE, a.mod_date_time, (select count(*) from gdd_hdr g where a.load_nbr = g.load_nbr) cant_guias  from OUTBD_LOAD a, outpt_OUTBD_LOAD b where a.load_nbr=b.load_nbr  and a.stat_code in ('80','90') and a.mod_date_time BETWEEN           TO_DATE('"+iFechaIni+" 00:00:00', 'DD-MM-YYYY HH24:MI:SS') AND  TO_DATE('"+iFechaFin+" 23:59:59', 'DD-MM-YYYY HH24:MI:SS') ");
			info("[sb]"+sb);

			pstmt         = dbconnOracle.prepareStatement(sb.toString());
			//pstmt.setInt(1, iFechaIni);
			//pstmt.setInt(2, iFechaFin);
			sb = new StringBuffer();
			ResultSet rs = pstmt.executeQuery();
			bw  = new BufferedWriter(new FileWriter(file1));
			bw.write("Carga;");
			bw.write("Bodega;");
			bw.write("Fecha;");
			bw.write("Tipo;");

			bw.write("Tran_nbr;");
			bw.write("Tienda;");
			bw.write("Guias;");
			bw.write("Detalles;");
			bw.write("Cant_Wms;");
			//bw.write("Recibido_Jda;");
			bw.write("Procesado_jda;");
			//bw.write("Diferencias;");
			//bw.write("Ajustes;");
			bw.write("Encontrados\n");

			while (rs.next()) {
				bw.write(rs.getString("LOAD_NBR") + ";");
				bw.write(rs.getString("WHSE") + ";");
				bw.write(rs.getString("mod_date_time") + ";");
				bw.write("CTL" + ";");

				

				if (rs.getString("LOAD_NBR") != null) {
					
					
					bw.write(ejecutarQuery2(limpiarCeros(rs.getString("LOAD_NBR")), rs.getString("cant_guias"),ejecutarQueryDetalle(limpiarCeros(rs.getString("LOAD_NBR")), rs.getString("cant_guias"), iFechaIni, iFechaFin, dbconnection), iFechaIni, iFechaFin, dbconnection));
				}
			}

			info("Archivos creados.");
		}
		catch (Exception e) {

			System.out.println(e.getMessage());
			info("[crearTxt1]Exception:"+e.getMessage());
		}
		finally {

			cerrarTodo(dbconnection,pstmt,bw);
			cerrarTodo(dbconnOracle, null, bw2);
		}
	}

	private static String ejecutarQuery2(String ctl, String cant_guias,String detalle, String iFechaIni2,  String iFechaFin2 ,Connection dbconnection) {

		StringBuffer sb         = new StringBuffer();
		PreparedStatement pstmt = null;

		try {

			sb = new StringBuffer();
			sb.append("Select NUMTRANSAC, NUMTIENDA, CDWMS, CANTREGDET, sum(cantidad) as detalle, sum(cantidad) as cantidad from exisbugd.exiff94 where FECTRANSAC = 20171205 AND NUMCARGA = "+ ctl + " group by NUMTRANSAC, NUMTIENDA, CDWMS, CANTREGDET" );
			
			pstmt = dbconnection.prepareStatement(sb.toString());
			ResultSet rs = pstmt.executeQuery();
			sb = new StringBuffer();

			boolean reg = false;
			do{
				reg = rs.next();
				if (reg){
					sb.append(rs.getString("NUMTRANSAC") + ";");
					sb.append(rs.getString("NUMTIENDA") + ";");
					sb.append(cant_guias + ";");
					sb.append(rs.getString("CANTREGDET")  + ";");
					sb.append(rs.getString("cantidad") + ";");
					//sb.append("" + ";");
					sb.append(rs.getString("CDWMS") + ";");
					//sb.append("" + ";");
					//sb.append("" + ";");
					sb.append("Encontrado en JDA" + "\n");
					

					
					
					break;
				}else{
					sb.append("-" + ";");
					sb.append("-" + ";");
					sb.append("-" + ";");
					sb.append("-" + ";");
					sb.append("-" + ";");
					//sb.append("" + ";");
					sb.append("-" + ";");
					//sb.append("-" + ";");
					//sb.append("-" + ";");
					sb.append("No encontrado en JDA" + "\n");
				}
			}
			while (reg);
		}
		catch (Exception e) {

			e.printStackTrace();
			info("[crearTxt2]Exception:"+e.getMessage());
		}
		finally {

			cerrarTodo(null,pstmt,null);
		}
		return sb.toString();
	}
	
	private static String ejecutarQueryDetalle(String ctl, String cant_guias, String iFechaIni2,  String iFechaFin2 ,Connection dbconnection) {

		StringBuffer sb         = new StringBuffer();
		PreparedStatement pstmt = null;
		String result = "";

		try {

			sb = new StringBuffer();
			sb.append("Select NUMTRANSAC, NUMTIENDA, CDWMS, sum(cantidad) as detalle, sum(cantidad) as cantidad from exisbugd.exiff94 where FECTRANSAC = 20171205 AND NUMCARGA = "+ ctl + " group by NUMTRANSAC, NUMTIENDA, CDWMS" );
			
			pstmt = dbconnection.prepareStatement(sb.toString());
			ResultSet rs = pstmt.executeQuery();
			sb = new StringBuffer();
			

			boolean reg = false;
			do{
				reg = rs.next();
				if (reg){
					//sb.append(rs.getString("cantidad") + ";");
					result = rs.getString("cantidad");
					
					break;
				}else{
					result = "-";
				}
			}
			while (reg);
		}
		catch (Exception e) {

			e.printStackTrace();
			info("[crearTxt2]Exception:"+e.getMessage());
		}
		finally {

			cerrarTodo(null,pstmt,null);
		}
		return result;
	}

	private static Connection crearConexion() {

		System.out.println("Creado conexion a ROBLE.");
		AS400JDBCDriver d = new AS400JDBCDriver();
		String mySchema = "RDBPARIS2";
		Properties p = new Properties();
		AS400 o = new AS400("roble.cencosud.corp","USRCOM", "USRCOM");
		Connection dbconnection = null;

		try {

			System.out.println("AuthenticationScheme: "+o.getVersion());
			dbconnection = d.connect (o, p, mySchema);
			System.out.println("Conexion a ROBLE CREADA.");
		}
		catch (Exception e) {

			System.out.println(e.getMessage());
		}
		return dbconnection;
	}

	private static Connection crearConexionOracle() {

		Connection dbconnection = null;

		try {

			Class.forName("oracle.jdbc.driver.OracleDriver");

			dbconnection = DriverManager.getConnection("jdbc:oracle:thin:@g500603svbbr:1521:REPORTMHN","CONWMS","CONWMS");

		}
		catch (Exception e) {

			e.printStackTrace();
		}
		return dbconnection;
	}

	private static String limpiarCeros(String str) {

		int iCont = 0;

		while (str.charAt(iCont) == '0') {

			iCont++;
		}
		return str.substring(iCont, str.length());
	}

	private static void cerrarTodo(Connection cnn, PreparedStatement pstmt, BufferedWriter bw){

		try {

			if (cnn != null) {

				cnn.close();
				cnn = null;
			}
		}
		catch (Exception e) {

			System.out.println(e.getMessage());
			info("[cerrarTodo]Exception:"+e.getMessage());
		}
		try {

			if (pstmt != null) {

				pstmt.close();
				pstmt = null;
			}
		}
		catch (Exception e) {

			System.out.println(e.getMessage());
			info("[cerrarTodo]Exception:"+e.getMessage());
		}
		try {

			if (bw != null) {

				bw.flush();
				bw.close();
				bw = null;
			}
		}
		catch (Exception e) {

			System.out.println(e.getMessage());
			info("[cerrarTodo]Exception:"+e.getMessage());
		}
	}

	private static void info(String texto){

		try {

			bw.write(texto+"\n");
			bw.flush();
		}
		catch (Exception e) {

			System.out.println("Exception:"+e.getMessage());
		}
	}

	private static String restarDias(String sDia, int iCantDias) {

		String sFormatoIn = "yyyyMMdd";
		String sFormatoOut = "dd-MM-yyyy";
		Calendar diaAux = null;
		String sDiaAux = null;
		SimpleDateFormat df = null;

		try {

			diaAux = Calendar.getInstance();
			df = new SimpleDateFormat(sFormatoIn);
			diaAux.setTime(df.parse(sDia));
			diaAux.add(Calendar.DAY_OF_MONTH, -iCantDias);
			df.applyPattern(sFormatoOut);
			sDiaAux = df.format(diaAux.getTime());
		}
		catch (Exception e) {

			info("[restarDias]error: " + e);
		}
		return sDiaAux;
	}
	
	private static String restarDias2(String sDia, int iCantDias) {

		String sFormatoIn = "yyyyMMdd";
		String sFormatoOut = "yyyy/MM/dd";
		Calendar diaAux = null;
		String sDiaAux = null;
		SimpleDateFormat df = null;

		try {

			diaAux = Calendar.getInstance();
			df = new SimpleDateFormat(sFormatoIn);
			diaAux.setTime(df.parse(sDia));
			diaAux.add(Calendar.DAY_OF_MONTH, -iCantDias);
			df.applyPattern(sFormatoOut);
			sDiaAux = df.format(diaAux.getTime());
		}
		catch (Exception e) {

			info("[restarDias]error: " + e);
		}
		return sDiaAux;
	}
	
	private static String restarDiasTxt(String sDia, int iCantDias) {

		String sFormatoIn = "yyyyMMdd";
		String sFormatoOut = "yyyyMMdd";
		Calendar diaAux = null;
		String sDiaAux = null;
		SimpleDateFormat df = null;

		try {

			diaAux = Calendar.getInstance();
			df = new SimpleDateFormat(sFormatoIn);
			diaAux.setTime(df.parse(sDia));
			diaAux.add(Calendar.DAY_OF_MONTH, -iCantDias);
			df.applyPattern(sFormatoOut);
			sDiaAux = df.format(diaAux.getTime());
		}
		catch (Exception e) {

			info("[restarDias]error: " + e);
		}
		return sDiaAux;
	}
}
