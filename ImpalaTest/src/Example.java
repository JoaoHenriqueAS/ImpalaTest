import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class Example {
	
	//JDBC DRIVER E URL BANCO
	static final String JDBC_DRIVER = "com.cloudera.impala.jdbc.Driver";
	static final String DB_URL = "jdbc:impala://cld-wn02.infra.elo:25000//elo_marketing";
	
	//USER E SENHA
	static final String USER = "02.000500";
	static final String PASS = "elo@2019";

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
			sql = "SELECT nome,\r\n" + 
					"         cpf\r\n" + 
					"FROM \r\n" + 
					"  (SELECT regexp_replace(nome,\r\n" + 
					"        '\\\\.+', ' ')as nome ,cpf ,data_cadastro\r\n" + 
					"  FROM \r\n" + 
					"    (SELECT cpf ,\r\n" + 
					"        nome ,\r\n" + 
					"        data_cadastro ,\r\n" + 
					"        RANK ()\r\n" + 
					"        OVER ( PARTITION BY cpf\r\n" + 
					"    ORDER BY cast(data_cadastro AS timestamp) ASC) AS rank_\r\n" + 
					"    FROM \r\n" + 
					"      (SELECT lpad(cast(cast(cpf AS BIGINT) AS STRING),\r\n" + 
					"         11,\r\n" + 
					"         '0') AS cpf ,nome ,data_cadastro\r\n" + 
					"      FROM elo_marketing.base_cadastro_manual\r\n" + 
					"      WHERE optin = 1\r\n" + 
					"              AND (\r\n" + 
					"          CASE\r\n" + 
					"          WHEN email <> \"\"\r\n" + 
					"              OR celular <> \"\" THEN\r\n" + 
					"          1\r\n" + 
					"          ELSE 0\r\n" + 
					"          END ) = 1\r\n" + 
					"              AND numerocartao_sha2 <> \"\"\r\n" + 
					"              AND origem NOT LIKE 'pefisa'\r\n" + 
					"      UNION\r\n" + 
					"      ALLSELECT lpad(cast(cast(pc.tx_cpf_number AS BIGINT) AS STRING),\r\n" + 
					"         11,\r\n" + 
					"         '0') AS cpf ,h.tx_name AS nome ,(\r\n" + 
					"          CASE\r\n" + 
					"          WHEN from_unixtime(unix_timestamp(a.created_at)-10800) < '2018-06-05 10:00:00' THEN\r\n" + 
					"          from_unixtime(unix_timestamp(a.created_at)-10800, 'yyyy-MM-dd')\r\n" + 
					"          ELSE from_unixtime(unix_timestamp(h.dt_created)-10800, 'yyyy-MM-dd')\r\n" + 
					"          END ) AS data_cadastro\r\n" + 
					"      FROM elo_api_user.user h\r\n" + 
					"      LEFT JOIN elo_api_user.physical_type_person pc\r\n" + 
					"          ON h.nr_id_physical_type_person = pc.nr_id_physical_type_person\r\n" + 
					"      LEFT JOIN elo_api_user.card f\r\n" + 
					"          ON trim(f.tx_id_user) = trim(h.tx_id_user)\r\n" + 
					"      LEFT JOIN elo_api_user.de_para_sha2 g\r\n" + 
					"          ON f.tx_card_sha1 = g.tx_card_sha1\r\n" + 
					"      LEFT JOIN elo_hub_promocoes.users a\r\n" + 
					"          ON a.cpf = pc.tx_cpf_number\r\n" + 
					"      LEFT JOIN \r\n" + 
					"        (SELECT a.user_id ,\r\n" + 
					"        a.hub_key ,\r\n" + 
					"        b.name AS aplicacao_origem ,\r\n" + 
					"        b.created_at\r\n" + 
					"        FROM elo_hub_promocoes.logs AS a\r\n" + 
					"        LEFT JOIN elo_hub_promocoes.hub_clients b\r\n" + 
					"            ON a.hub_key = b.client_id\r\n" + 
					"        WHERE a.action = 'signUp' ) z\r\n" + 
					"            ON trim(h.tx_user_name) = trim(z.user_id)\r\n" + 
					"        LEFT JOIN elo_api_user.user_agreement_term c\r\n" + 
					"            ON trim(h.tx_id_user) = trim(c.tx_id_user)\r\n" + 
					"        LEFT JOIN elo_hub_promocoes.agreements j\r\n" + 
					"            ON j.user_id = a.id\r\n" + 
					"        WHERE f.tx_card_sha256 is NOT NULL\r\n" + 
					"                AND ( a.newsletter = true\r\n" + 
					"                OR c.dt_accepted_term is NOT null\r\n" + 
					"                OR j.created_at is NOT NULL )\r\n" + 
					"        GROUP BY 1,2,3 ) x ) k\r\n" + 
					"        WHERE rank_ = 1) AS PORTADOR_ELO\r\n" + 
					"      WHERE data_cadastro\r\n" + 
					"        BETWEEN '2019-10-30'\r\n" + 
					"        AND '2019-10-31'\r\n" + 
					"        AND TRIM(nome) != ''\r\n" + 
					"        AND TRIM(nome) != '0'\r\n" + 
					"        AND trim(nome) != '.'\r\n" + 
					"        AND trim(cpf) != ''\r\n" + 
					"ORDER BY data_cadastro";
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
