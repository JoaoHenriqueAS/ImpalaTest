import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class Example {
	
	//JDBC DRIVER E URL BANCO
	static final String JDBC_DRIVER = "com.cloudera.impala.jdbc.Driver";
	static final String DB_URL = "jdbc:impala://[host]:[port]//elo_marketing";
	
	//USER E SENHA
	static final String USER = "*******";
	static final String PASS = "*******";

	public static void main(String[] args) {
		Connection conn = null;
		Statement stmt = null;
		
		try {
			//Driver JDBC
			Class.forName("com.cloudera.impala.jdbc.Driver");
			
			//Abrindo conexão
			System.out.println("Conectando ao banco...");
			conn = DriverManager.getConnection(DB_URL, USER, PASS);
			System.out.println("Conexão realizada com sucesso!!");
			
			//Executar a query
			System.out.println("Creating Statement... \n");
			stmt = conn.createStatement();
			String sql;
			sql = "SELECT ";
			ResultSet rs = stmt.executeQuery(sql);
			
			//Extração de dados
			while(rs.next()) {
				String nome = rs.getString(1);
				String cpf = rs.getString(2);
				
				System.out.println(nome + ";" + cpf);
			}
			//Limpar dados
			rs.close();
			stmt.close();
			conn.close();			
		} catch(SQLException se) {
			se.printStackTrace();
		} catch(Exception e) {
			e.printStackTrace();
		} finally {
			//Fechar conexão
			try {
				if(stmt != null) {
					stmt.close();
				}
			} catch(SQLException se2) {
			}
			try {
				if(conn != null) {
					conn.close();
				}
			} catch(SQLException se) {
				se.printStackTrace();
			}
		}
	}
}
