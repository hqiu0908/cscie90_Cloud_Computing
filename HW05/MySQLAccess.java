/**
 * 
 */
package mysql.hu.edu;

/**
 * @author hqiu
 *
 */
public class MySQLAccess {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		PersonDAO dao = new PersonDAO();
	    try {
			dao.readDataBase();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
