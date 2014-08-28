import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;

import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.Compression;
import org.apache.cassandra.thrift.CqlResult;
import org.apache.cassandra.thrift.CqlRow;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.SchemaDisagreementException;
import org.apache.cassandra.thrift.TimedOutException;
import org.apache.cassandra.thrift.UnavailableException;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;


/* 
 * TPC-C Transactions on Cassandra
 * @author Wei HU
 * @date 2014-08-25 
 */
public class TPCC {
	
	/* Database configuration */
	private String DB_KEYSPACE = "tpcc";
	private String DB_ADDRESS = "localhost";
	private int DB_PORT = 9160;
	/* db variables */
	private TTransport DB_TR = null;
	Cassandra.Client client = null;
	public static int COUNT_WARE = 2;
	/* The constants as specified */
	public static final int MAXITEMS = 100000;
	public static final int CUST_PER_DIST = 3000;
	public static final int DIST_PER_WARE = 10;
	public static final int ORD_PER_DIST = 3000;
	/* NURand */
	public static final int A_C_LAST = 255;
	public static final int A_C_ID = 1023;
	public static final int A_OL_I_ID = 8191;
	public static final int C_C_LAST = randomInt(0, A_C_LAST);
	public static final int C_C_ID = randomInt(0, A_C_ID);
	public static final int C_OL_I_ID = randomInt(0, A_OL_I_ID);
	/* constant names of the column families */
	public static String WAREHOUSE = "warehouse";
	public static String DISTRICT = "district";
	public static String CUSTOMER = "customer";
	public static String HISTORY = "history";
	public static String ORDER = "order";
	public static String NEWORDER = "new_order";
	public static String ORDERLINE = "order_line";
	public static String ITEM = "item";
	public static String STOCK = "stock";
	/* Encoding */
	public static String UTF8 = "UTF-8";
	
	/*
	 * Description: This function convert String type to ByteBuffer
	 * Argument: value - the string to convert
	 */
	public ByteBuffer toByteBuffer(String value) {
		return ByteBuffer.wrap(value.getBytes());
	}
	
	public ByteBuffer toByteBuffer(int value) {
		return ByteBuffer.wrap(String.valueOf(value).getBytes());
	}
	
	public ByteBuffer toByteBuffer(float value) {
		return ByteBuffer.wrap(String.valueOf(value).getBytes());
	}

	/*
	 * Description: This function convert ByteBuffer type to String
	 * Argument: buffer - the ByteBuffer to convert
	 */
	public String toString(ByteBuffer buffer) {
		byte[] bytes = new byte[buffer.remaining()];
		buffer.get(bytes);
		return new String(bytes);
	}
	
	public String toString(byte[] bytes) {
		return new String(bytes);
	}
	
	/* 
	 * Description: This function return next random integer within the range 
	 */
	public static int randomInt(int min, int max) {
		return new Random().nextInt(max + 1) % (max - min + 1) + min;
	}
	
	/*
	 *  Description: This function return next random float within the range
	 */
	public static float randomFloat(float min, float max) {
		return new Random().nextFloat() * (max - min) + min;
	}
	
	/*
	 * Description: This function return generated NR random number
	 */
	public static int NURand(int A, int x, int y) {
		int c = 0;
		switch (A) {
		case A_C_LAST:
			c = C_C_LAST;
			break;
		case A_C_ID:
			c = C_C_ID;
			break;
		case A_OL_I_ID:
			c = C_OL_I_ID;
			break;
		default:
		}
		return (((randomInt(0, A) | randomInt(x, y)) + c) % (y - x + 1)) + x;
	}
	
	/*
	 * Description: This function set up connection and return Cassandra.Client instance
	 */
	public void getConnection() throws InvalidRequestException, TException {

		if (DB_TR == null) {
			DB_TR = new TFramedTransport(new TSocket(DB_ADDRESS, DB_PORT));
		}

		if (!DB_TR.isOpen()) {
			DB_TR.open();
		}

		client = new Cassandra.Client(new TBinaryProtocol(DB_TR));
		client.set_keyspace(DB_KEYSPACE);

	}

	/*
	 * Description: This function close the database connection
	 */
	public void closeConnection() throws TTransportException {

		if (client != null) {
			client = null;
		}
		
		if (DB_TR != null) {
			DB_TR.flush();
			DB_TR.close();
			DB_TR = null;
		}

	}
	
	private CqlResult executeQuery(String query) throws InvalidRequestException, UnavailableException, TimedOutException, SchemaDisagreementException, UnsupportedEncodingException, TException {
		return client.execute_cql_query(toByteBuffer(query), Compression.NONE);
	}
	
	
	/*
	 * 
	 */
	public void Neworder(int w_id, int d_id) throws InvalidRequestException, TException, UnsupportedEncodingException {
		
		/* Set up database connection */
		getConnection();
		
		/* local variables */
		int d_next_o_id = 0, o_id = 0, o_all_local = 1;
		int c_id = NURand(A_C_ID, 1,3000), o_ol_cnt = randomInt(5, 15);
		float w_tax = 0, d_tax = 0, c_discount = 0;
		String o_entry_d = (new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")).format(new Date(System.currentTimeMillis()));
		String query;
		CqlResult result;
		boolean valid = true;
		
		/* retrieve warehouse information  */
		result = executeQuery("SELECT w_tax FROM warehouse WHERE key='" + w_id + "'");
		for (CqlRow row : result.getRows()) {
			for (Column column : row.getColumns() ) {
				switch(toString(column.getName())) {
				case "w_tax": w_tax = Float.valueOf(toString(column.getValue())); break;
				default: System.out.println("Error: Neworder - fail to retrieve warehouse information");
				}
			}
		}
		
		/* retrieve customer information  */
		String customer_key = w_id + "_" + d_id + "_" + c_id;
		String c_last = null, c_credit = null;
		result = executeQuery("SELECT c_discount, c_credit, c_last FROM customer WHERE key='" + customer_key + "'");
		for (CqlRow row : result.getRows()) {
			for (Column column : row.getColumns() ) {
				switch(toString(column.getName())) {
				case "c_discount": c_discount = Float.valueOf(toString(column.getValue())); break;
				case "c_last": c_last = toString(column.getValue()); break;
				case "c_credit": c_credit = toString(column.getValue()); break;
				default: System.out.println("Error: Neworder - fail to retrieve customer information");
				}
			}
		}
		
		/* retrieve district information  */
		String district_key = w_id + "_" + d_id;
		result = executeQuery("SELECT d_next_o_id, d_tax FROM district WHERE key='" + district_key + "'");
		for (CqlRow row : result.getRows()) {
			for (Column column : row.getColumns() ) {
				switch(toString(column.getName())) {
				case "d_next_o_id": d_next_o_id = Integer.valueOf(toString(column.getValue())); break;
				case "d_tax": d_tax = Float.valueOf(toString(column.getValue())); break;
				default: System.out.println("Error: Neworder - fail to retrieve district information");
				}
			}
		}

		/* increase d_next_o_id by one */
		query = "UPDATE district SET d_next_o_id = '" + String.valueOf(d_next_o_id+1)  + "' WHERE KEY='" + district_key + "'";
		executeQuery(query);
		
		/* build supware for each order line */
		int supware[] = new int[o_ol_cnt];
		int ol_i_ids[] = new int[o_ol_cnt];
		int ol_quantities[] = new int[o_ol_cnt];
		int s_quantities[] = new int[o_ol_cnt];
		String i_names[] = new String[o_ol_cnt];
		float i_prices[] = new float[o_ol_cnt];
		float ol_amounts[] = new float[o_ol_cnt];
		char bg[] = new char[o_ol_cnt];
		for (int ol_number = 1; ol_number<=o_ol_cnt; ol_number++) {
			
			int ol_supply_w_id; 
			/* 99% of supply are from home stock*/
			if ((new Random()).nextInt(100) == 0 && COUNT_WARE > 1) {
				int supply_w_id = randomInt(1, COUNT_WARE); 
				while (supply_w_id == w_id) {
					supply_w_id = randomInt(1, COUNT_WARE); 
				}
				ol_supply_w_id = supply_w_id;
			} else {
				ol_supply_w_id = w_id;
			}
			
			if (ol_supply_w_id != w_id) {
				o_all_local = 0; 
			}
			supware[ol_number - 1] = ol_supply_w_id;
			ol_i_ids[ol_number - 1] = NURand(A_OL_I_ID,1,100000);
			/* rbk is used for 1% of error */
			int rbk = randomInt(1, 100);
			if(rbk == 1) {
				ol_i_ids[ol_number - 1] = randomInt(200001, 300000);
			}
		}
		
		/* assign d_next_o_id to o_id */
		o_id = d_next_o_id;
		String order_key = w_id + "_" + d_id + "_" + o_id;
		/* insert into new order table and order table*/
		query = "INSERT INTO order (key, o_id,  o_d_id, o_w_id, o_c_id, o_entry_id, o_carrier_id, o_ol_cnt, o_all_local) "
				+ "VALUES ('" + order_key  + "', '" + o_id + "', '" + d_id + "', '" + w_id + "', '" + c_id + "', '" + o_entry_d 
				+ "', 'NULL', '" + o_ol_cnt + "', '" + o_all_local + "')";
		executeQuery(query);
		
		query = "INSERT INTO new_order (key, o_id,  o_d_id, o_w_id, o_c_id) "
				+ "VALUES ('" + order_key  + "', '" + o_id + "', '" + d_id + "', '" + w_id + "', '" + c_id +  "')";
		executeQuery(query);
		
		/* for each order in the order line*/
		for (int ol_number = 1; ol_number <= o_ol_cnt; ol_number++) {

			int ol_supply_w_id = supware[ol_number - 1]; 
			int ol_i_id = ol_i_ids[ol_number - 1]; 
			int ol_quantity = randomInt(1, 10);
			int s_quantity = 0;
			float i_price = 0.0f, ol_amount = 0.0f;
			String i_name = null, ol_dist_info = null, i_data = null;
			
			/* retrieve item information */
			result = executeQuery("SELECT i_price, i_name, i_data FROM item WHERE i_id='" + ol_i_id + "'");
			if (result.getRowsSize() == 0) {
				/* roll back new order and return */
				executeQuery("DELETE FROM order WHERE key='" + order_key + "'");
				executeQuery("DELETE FROM new_order WHERE key='" + order_key + "'");
				for (int i = 1; i < o_ol_cnt; i++) {
					String order_line_key = w_id + "_" + d_id + "_" + o_id + "_" + i;
					executeQuery("DELETE FROM order_line WHERE key='" + order_line_key + "'");
				}
				executeQuery("UPDATE district SET d_next_o_id = '" + String.valueOf(d_next_o_id-1)  + "' WHERE KEY='" + district_key + "'");
				valid = false;
				break;
			}
			
			for (CqlRow row : result.getRows()) {
				for (Column column : row.getColumns() ) {
					switch(toString(column.getName())) {
						case "i_price": i_price = Float.valueOf(toString(column.getValue())); break;
						case "i_name": i_name = toString(column.getValue()); break;
						case "i_data": i_data = toString(column.getValue()); break;
						default: System.out.println("Error: Neworder - fail to retrieve item information");
					}
				}
			}
			
			/* retrieve stock information */
			String stock_key = ol_supply_w_id + "_" + ol_i_id;
			String s_dist[] = new String[DIST_PER_WARE];
			String s_data = null;
			
			query = "SELECT s_quantity, s_dist_01, s_dist_02, s_dist_03, s_dist_04, s_dist_05, s_dist_06, s_dist_07, s_dist_08, "
					+ "s_dist_09, s_dist_10, s_data FROM stock WHERE key='" + stock_key + "'";
			result = executeQuery(query);
			for (CqlRow row : result.getRows()) {
				for (Column column : row.getColumns() ) {
					switch(toString(column.getName())) {
						case "s_quantity": s_quantity = Integer.valueOf(toString(column.getValue())); break;
						case  "s_dist_01": s_dist[0] =  toString(column.getValue()); break;
						case  "s_dist_02": s_dist[1] =  toString(column.getValue()); break;
						case  "s_dist_03": s_dist[2] =  toString(column.getValue()); break;
						case  "s_dist_04": s_dist[3] =  toString(column.getValue()); break;
						case  "s_dist_05": s_dist[4] =  toString(column.getValue()); break;
						case  "s_dist_06": s_dist[5] =  toString(column.getValue()); break;
						case  "s_dist_07": s_dist[6] =  toString(column.getValue()); break;
						case  "s_dist_08": s_dist[7] =  toString(column.getValue()); break;
						case  "s_dist_09": s_dist[8] =  toString(column.getValue()); break;
						case  "s_dist_10": s_dist[9] =  toString(column.getValue()); break;
						case  "s_data": s_data =  toString(column.getValue()); break;
						default: System.out.println("Error: Neworder - fail to retrieve stock information");
					}
				}
			}
			ol_dist_info = s_dist[d_id-1];
			

			if ( i_data != null && s_data != null && (i_data.indexOf("original") != -1) && (s_data.indexOf("original") != -1) ) {
				bg[ol_number-1] = 'B'; 
			} else {
				bg[ol_number-1] = 'G';
			}
			
			if (s_quantity > ol_quantity) {
				s_quantity = s_quantity - ol_quantity;
			} else {
				s_quantity = s_quantity - ol_quantity + 91;
			}

			/* update stock quantity */
			executeQuery("UPDATE stock SET s_quantity='" + s_quantity + "' WHERE key='" + stock_key  + "'");
			
			/* calculate order-line amount*/
			ol_amount = ol_quantity * i_price *(1+w_tax+d_tax) *(1-c_discount); 
			
			/* insert into order line table */
			String order_line_key = w_id + "_" + d_id + "_" + o_id + "_" + ol_number;
			query = "INSERT INTO order_line (key, ol_o_id,  ol_d_id, ol_w_id, ol_number, ol_i_id, ol_supply_w_id, ol_delivery_id, ol_quantity, ol_amount, ol_dist_info) "
					+ "VALUES ('" + order_line_key  + "', '" + o_id + "', '" + d_id + "', '" + w_id + "', '" + ol_number
					+ "' ,'" + ol_i_id + "', '" + ol_supply_w_id + "', 'NULL', '" + ol_quantity + "', '" + ol_amount 
					+ "', '" + ol_dist_info + "')";
			executeQuery(query);
			
			i_names[ol_number - 1] = i_name;
			i_prices[ol_number - 1] = i_price;
			ol_amounts[ol_number - 1] = ol_amount;
			ol_quantities[ol_number - 1] = ol_quantity;
			s_quantities[ol_number - 1] = s_quantity;
		}
		
		/* output */
		System.out.println("==============================New Order==================================");
		System.out.println("Warehouse: " + w_id + "\tDistrict: " + d_id);
		if (valid) {
			System.out.println("Customer: " + c_id + "\tName: " + c_last + "\tCredit: " + c_credit + "\tDiscount: " + c_discount);
			System.out.println("Order Number: " + o_id + " OrderId: " + o_id + " Number_Lines: " + o_ol_cnt + " W_tax: " + w_tax + " D_tax: " + d_tax + "\n");
			System.out.println("Supp_W Item_Id           Item Name     ol_q s_q  bg Price Amount");
			for (int i = 0; i < o_ol_cnt; i++) {
				System.out.println( String.format("  %4d %6d %24s %2d %4d %3c %6.2f %6.2f",
						supware[i], ol_i_ids[i], i_names[i], ol_quantities[i], s_quantities[i], bg[i], i_prices[i], ol_amounts[i]));
			}
		} else {
			System.out.println("Customer: " + c_id + "\tName: " + c_last + "\tCredit: " + c_credit + "\tOrderId: " + o_id);
			System.out.println("Exection Status: Item number is not valid");
		}
		System.out.println("=========================================================================");
		
        /* Close database connection */
		closeConnection();
	}
	
	
	/*
	 * Function name: Payment
	 * Description: The Payment business transaction updates the customer's balance and reflects the payment
	 * 				on the district and warehouse sales statistics.
	 * Argument: 	d_id － randomly selected within [1 .. 10]
	 * 				c_id_or_c_last － 60% c_last random NURand(255,0,999) | 40% c_id - random NURand(1023,1,3000)
	 */
	public void Payment(int w_id, int d_id, Object c_id_or_c_last) throws InvalidRequestException, TException,
			UnsupportedEncodingException {
		
		/* Set up database connection */
		getConnection();
		
		/* c_id or c_last */
		Boolean byname = false;
		if (c_id_or_c_last instanceof String) {
			byname = true;
		} 
		/* local variables */
		float h_amount = randomFloat(0, 5000);
		int x = randomInt(1, 100);
		/*  the customer resident warehouse is the home 85% , remote 15% of the time  */
		int c_d_id, c_w_id;
		if (x <= 85 ) { 
			c_w_id = w_id;
			c_d_id = d_id;
		} else {
			c_d_id = randomInt(1, 10);
			do {
				c_w_id = randomInt(1, COUNT_WARE);
			} while (c_w_id == w_id && COUNT_WARE > 1);
		}
		String h_date = (new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")).format(new Date(System.currentTimeMillis()));
		CqlResult result;
		
		
		/* retrieve and update warehouse w_ytd */
		float w_ytd = 0;
		String w_name = null, w_street_1 = null, w_street_2 = null, w_city = null, w_state = null, w_zip = null;
		result = executeQuery("SELECT w_ytd, w_name, w_street_1, w_street_2, w_city, w_state, w_zip FROM warehouse WHERE key='" + w_id + "'");
		for (CqlRow row : result.getRows()) {
			for (Column column : row.getColumns() ) {
				switch(toString(column.getName())) {
					case "w_ytd": w_ytd = Float.valueOf(toString(column.getValue())); break;
					case "w_name": w_name = toString(column.getValue()); break;
					case "w_street_1": w_street_1 = toString(column.getValue()); break;
					case "w_street_2": w_street_2 = toString(column.getValue()); break;
					case "w_city": w_city = toString(column.getValue()); break;
					case "w_state": w_state = toString(column.getValue()); break;
					case "w_zip": w_zip = toString(column.getValue()); break;
					default: System.out.println("Error: Payment - fail to retrieve warehouse information");
				}
			}
		}
		w_ytd += h_amount;
		executeQuery("UPDATE warehouse SET w_ytd='" + w_ytd + "' WHERE key='" + w_id  + "'");
		
		
		/* retrieve and update district d_ytd */
		float d_ytd = 0;
		String d_name = null,  d_street_1 = null, d_street_2 = null, d_city = null, d_state = null, d_zip = null;
		String district_key = w_id + "_" + d_id;
		result = executeQuery("SELECT d_ytd, d_name, d_street_1, d_street_2, d_city, d_state, d_zip FROM district WHERE key='" + district_key + "'");
		for (CqlRow row : result.getRows()) {
			for (Column column : row.getColumns() ) {
				switch(toString(column.getName())) {
					case "d_ytd": d_ytd = Float.valueOf(toString(column.getValue())); break;
					case "d_name": d_name = toString(column.getValue()); break;
					case "d_street_1": d_street_1 = toString(column.getValue()); break;
					case "d_street_2": d_street_2 = toString(column.getValue()); break;
					case "d_city": d_city = toString(column.getValue()); break;
					case "d_state": d_state = toString(column.getValue()); break;
					case "d_zip": d_zip = toString(column.getValue()); break;
					default: System.out.println("Error: Payment - fail to retrieve district information");
				}
			}
		}
		d_ytd += h_amount;
		executeQuery("UPDATE warehouse SET d_ytd='" + d_ytd + "' WHERE key='" + district_key  + "'");
		
		/* retrieve customer information */
		float c_balance = 0.0f;
		String c_data = null, h_data = null, c_first = null, c_middle = null, c_last = null;
		String c_street_1 = null, c_street_2 = null, c_city = null, c_state = null, c_zip = null;
		String c_phone = null, c_credit = null, c_credit_lim = null, c_since = null;
		int c_id = 0;
		String customer_key = null;
		if (byname) {
			result = executeQuery("SELECT c_first, c_middle, c_last, c_id, c_balance, c_credit, c_data, c_street_1, c_street_2, "
					+ "c_city, c_state, c_zip, c_phone, c_credit, c_credit_lim, c_since FROM customer WHERE c_w_id='" + c_w_id 
					+ "'AND c_d_id='" + c_d_id + "'AND c_last='" + (String)c_id_or_c_last + "'");
			/* TODP: ORDER BY c_first and get midpoint */
			for (CqlRow row : result.getRows()) {
				customer_key = toString(row.getKey());
				for (Column column : row.getColumns()) {
					switch (toString(column.getName())) {
					case "c_id": c_id = Integer.valueOf(toString(column.getValue())); break;
					case "c_first": c_first = toString(column.getValue()); break;
					case "c_middle": c_middle = toString(column.getValue()); break;
					case "c_last": c_last = toString(column.getValue()); break;
					case "c_city": c_city = toString(column.getValue()); break;
					case "c_state": c_state = toString(column.getValue()); break;
					case "c_zip": c_zip = toString(column.getValue()); break;
					case "c_phone": c_phone = toString(column.getValue()); break;
					case "c_balance": c_balance = Float.valueOf(toString(column.getValue())); break;
					case "c_credit": c_credit = toString(column.getValue()); break;
					case "c_credit_lim": c_credit_lim = toString(column.getValue()); break;
					case "c_since": c_since = toString(column.getValue()); break;
					case "c_data": c_data = toString(column.getValue()); break;
					default: System.out.println("Error: Payment - fail to retrieve customer information");
					}
				}
				break;
			}
		} else {
			customer_key = c_w_id + "_" + c_d_id + "_" + c_id_or_c_last;
			result = executeQuery("SELECT c_balance, c_credit, c_data, c_first, c_middle, c_last, c_street_1, c_street_2, "
					+ "c_city, c_state, c_zip, c_phone, c_credit, c_credit_lim, c_since FROM customer WHERE key='" + customer_key + "'");
			c_id = (int) c_id_or_c_last;
			for (CqlRow row : result.getRows()) {
				for (Column column : row.getColumns()) {
					switch (toString(column.getName())) {
					case "c_first": c_first = toString(column.getValue()); break;
					case "c_middle": c_middle = toString(column.getValue()); break;
					case "c_last": c_last = toString(column.getValue()); break;
					case "c_street_1": c_street_1 = toString(column.getValue()); break;
					case "c_street_2": c_street_2 = toString(column.getValue()); break;
					case "c_city": c_city = toString(column.getValue()); break;
					case "c_state": c_state = toString(column.getValue()); break;
					case "c_zip": c_zip = toString(column.getValue()); break;
					case "c_phone": c_phone = toString(column.getValue()); break;
					case "c_balance": c_balance = Float.valueOf(toString(column.getValue())); break;
					case "c_credit": c_credit = toString(column.getValue()); break;
					case "c_credit_lim": c_credit_lim = toString(column.getValue()); break;
					case "c_since": c_since = toString(column.getValue()); break;
					case "c_data": c_data = toString(column.getValue()); break;
					default: System.out.println("Error: Payment - fail to retrieve customer information");
					}
				}
			}
		}
		
		
		c_balance -= h_amount;
		h_data = w_name + "    " + d_name;
		if (c_credit.equals("BC")) {
			String c_new_data = String.format("| %4d %2d %4d %2d %4d $%7.2f %12s %24s", 
					c_id,c_d_id, c_w_id, d_id, w_id, h_amount, h_date, h_data);
			c_new_data += c_data;
			
			/* update customer c_balance， c_data */
			executeQuery("UPDATE customer SET c_balance='" + c_balance + "', c_data='" + c_new_data + "' WHERE key='" + customer_key  + "'");
			
		} else {
			/* update customer c_balance */
			executeQuery("UPDATE customer SET c_balance='" + c_balance + "' WHERE key='" + customer_key  + "'");

		}
		
		
		/* retrieve history key */
		String history_key = String.valueOf(System.currentTimeMillis());
		/* insert into history table */
		executeQuery("INSERT INTO history (key, h_c_d_id,  h_c_w_id, h_c_id, h_d_id, h_w_id, h_date, h_amount, h_data) "
				+ "VALUES ('" + history_key + "', '" + c_d_id + "', '"  + c_w_id + "', '"  + c_id + "', '"  + d_id + "', '" 
				+ w_id + "', '" + h_date + "', '"  + h_amount + "', '"  + h_data + "')");
		
		/* output */
		System.out.println("==============================Payment====================================");
		System.out.println("Date: " + h_date + " District: " + d_id);
		System.out.println("Warehouse: " + w_id + "\t\t\tDistrict");
		System.out.println(w_street_1 + "\t\t\t\t" + d_street_1);
		System.out.println(w_street_2 + "\t\t\t" + d_street_2);
		System.out.println(w_city + " " + w_state + " " + w_zip + "\t" + d_city + " " + d_state + " " + d_zip);
		System.out.println("");
		System.out.println("Customer: " + c_id + "\tCustomer-Warehouse: " + c_w_id + "\tCustomer-District: " + c_d_id);
		System.out.println("Name:" + c_first + " " + c_middle + " " + c_last + "\tCust-Since:" + c_since);
		System.out.println(c_street_1 + "\t\t\tCust-Credit:" + c_credit);
		System.out.println(c_street_2);
		System.out.println(c_city + " " + c_state + " " + c_zip + " \tCust-Phone:" + c_phone);
		System.out.println("");
		System.out.println("Amount Paid:" + h_amount  + "\t\t\tNew Cust-Balance: " + c_balance);
		System.out.println("Credit Limit:" + c_credit_lim);
		System.out.println("");
		if (c_credit.equals("BC")) {
			c_data = c_data.substring(0, 200);
		} 
		int length = c_data.length();
		int n = 50;
		int num_line = length / n;
		if (length % n != 0) num_line += 1;
		System.out.println( "Cust-data: \t" + c_data.substring(0, n));
		for (int i = 1; i < num_line - 1; i++) {
			System.out.println("\t\t" + c_data.substring(n*i, n*(i+1)));
		}
		System.out.println("\t\t" + c_data.substring(n*(num_line-1)));
		System.out.println("=========================================================================");
		
		
		/* Close database connection */
		closeConnection();
	}
	
	
	/*
	 * Function name: Orderstatus
	 * Description: The Order-Status business transaction queries the status of a customer's last order. 
	 * Argument: 	d_id － randomly selected within [1 .. 10]
	 * 				c_id_or_c_last - 60% c_last random NURand(255,0,999) | 40% c_id random NURand(1023,1,3000) 
	 */
	public void Orderstatus(int d_id, Object c_id_or_c_last) throws InvalidRequestException, TException, UnsupportedEncodingException {
		
		/* Set up database connection */
		getConnection();
		
		/* local variables */
		Boolean byname = false;
		int w_id = randomInt(1, COUNT_WARE);
		float c_balance = 0.0f;
		String c_first = null, c_middle = null, c_last = null;
		
		if (c_id_or_c_last instanceof String) {
			byname = true;
		} 
		
		CqlResult result;
		
		
		/* retrieve customer information */
		String customer_key = null;
		int c_id = 0;
		if (byname) {
			/* TODO: ORDER BY c_first and Choose the middle point */
			result = executeQuery("SELECT c_id, c_balance, c_first, c_middle, c_last FROM customer WHERE c_w_id='" + w_id 
					+ "'AND c_d_id='" + d_id + "'AND c_last='" + (String)c_id_or_c_last + "'");
			for (CqlRow row : result.getRows()) {
				customer_key = toString(row.getKey());
				for (Column column : row.getColumns()) {
					switch (toString(column.getName())) {
					case "c_id": c_id = Integer.valueOf(toString(column.getValue())); break;
					case "c_balance": c_balance = Float.valueOf(toString(column.getValue())); break;
					case "c_first": c_first = toString(column.getValue()); break;
					case "c_middle": c_middle = toString(column.getValue()); break;
					case "c_last": c_last = toString(column.getValue()); break;
					default: System.out.println("Error: Payment - fail to retrieve customer information");
					}
				}
				break;
			}
		} else {
			c_id = (int) c_id_or_c_last;
			customer_key = w_id + "_" + d_id + "_" + c_id;
			result = executeQuery("SELECT c_balance, c_balance, c_first, c_middle, c_last FROM customer WHERE key='" + customer_key + "'");
			for (CqlRow row : result.getRows()) {
				for (Column column : row.getColumns()) {
					switch (toString(column.getName())) {
					case "c_balance": c_balance = Float.valueOf(toString(column.getValue())); break;
					case "c_first": c_first = toString(column.getValue()); break;
					case "c_middle": c_middle = toString(column.getValue()); break;
					case "c_last": c_last = toString(column.getValue()); break;
					default: System.out.println("Error: Orderstatus - fail to retrieve customer information");
					}
				}
			}
		}
		
		/* retrieve an order */
		int o_id = 0;
		String o_carrier_id = null;
		String o_entry_d = null;
		/* TODO: ORDER BY o_id DESC */
		result = executeQuery("SELECT o_id, o_carrier_id, o_entry_d FROM order WHERE o_w_id='" + w_id 
				+ "'AND o_d_id='" + d_id  + "'AND o_c_id='" + c_id + "'");
		for (CqlRow row : result.getRows()) {
			customer_key = toString(row.getKey());
			for (Column column : row.getColumns()) {
				switch (toString(column.getName())) {
				case "o_id": o_id = Integer.valueOf(toString(column.getValue())); break;
				case "o_carrier_id": o_carrier_id = toString(column.getValue()); break;
				case "o_entry_d": o_entry_d = toString(column.getValue()); break;
				default: System.out.println("Error: Payment - fail to retrieve order information");
				}
			}
			break;
		}
		
		/* retrieve order_line information */
		int ol_i_id = 0, ol_supply_w_id = 0, ol_quantity = 0;
		float ol_amount = 0.0f;
		result = executeQuery("SELECT ol_i_id, ol_supply_w_id, ol_quantity, ol_amount FROM order_line "
				+ " WHERE ol_w_id='" + w_id  + "' AND ol_d_id='" + d_id +  "' AND ol_o_id='" + o_id + "'");
		
		/* output */
		System.out.println("==============================Order Status===============================");
		System.out.println("Warehouse: " + w_id + " District: " + d_id);
		System.out.println("Customer: " + c_id + " Name: " + c_first + " " + c_middle +  " " + c_last);
		System.out.println("Cust-Balance: $ " + c_balance);
		System.out.println("");
		System.out.println("Order-Number: " + o_id + " Entry-Date: " + o_entry_d + " Carrier-Id: " + o_carrier_id);
		System.out.println("Supply-W\tItem-ID\t\tQty\tAmount");
		for (CqlRow row : result.getRows()) {
			customer_key = toString(row.getKey());
			for (Column column : row.getColumns()) {
				switch (toString(column.getName())) {
				case "ol_i_id": ol_i_id = Integer.valueOf(toString(column.getValue())); break;
				case "ol_supply_w_id": ol_supply_w_id = Integer.valueOf(toString(column.getValue())); break;
				case "ol_quantity": ol_quantity = Integer.valueOf(toString(column.getValue())); break;
				case "ol_amount": ol_amount = Float.valueOf(toString(column.getValue())); break;
				default: System.out.println("Error: Payment - fail to retrieve order_line information");
				}
			}
			System.out.println(ol_supply_w_id + "\t\t" + ol_i_id + "\t\t" + ol_quantity + "\t" + ol_amount);
		}
		System.out.println("=========================================================================");
		
		/* Close database connection */
		closeConnection();
	}
	
	
	/*
	 * Function name: Delivery
	 * Description: The Delivery business transaction consists of processing a batch of 10 new (not yet delivered) orders.
	 * 				Each order is processed (delivered) in full within the scope of a read-write database transaction.
	 * Argument: o_carrier_id - randomly selected within [1 .. 10]
	 */
	public void Delivery(int o_carrier_id) throws InvalidRequestException, TException, UnsupportedEncodingException {
		
		/* Set up database connection */
		getConnection();
		
		/* local variables */
		int w_id = randomInt(1, COUNT_WARE);
		int d_id = randomInt(1, DIST_PER_WARE);
		CqlResult result;
		
		/* choose an new order */
		int no_o_id = 0;
		String new_order_key = null;
		/* TODO: ORDER BY no_o_id ASC*/
		result = executeQuery("SELECT no_o_id FROM new_order WHERE no_d_id='" + d_id + "' AND no_w_id='" + w_id + "'");
		for (CqlRow row : result.getRows()) {
			/*  If no matching row is found, then the delivery of an order for this district is skipped. */
			if (row.getColumnsSize() == 0) {
				return;
			}
			new_order_key = toString(row.getKey());
			for (Column column : row.getColumns() ) {
				switch(toString(column.getName())) {
				case "no_o_id": no_o_id = Integer.valueOf(toString(column.getValue())); break;
				default: System.out.println("Error: Delivery - fail to retrieve new_order information");
				}
			}
			break;
		}
		/* delete this new order for delivery */
		executeQuery("DELETE FROM new_order WHERE key='" + new_order_key + "'");
		
		/* get the customer id for this order */
		String customer_key = null;
		result = executeQuery("SELECT o_c_id FROM order WHERE o_id='" + no_o_id 
				+ "' AND o_d_id='" + d_id + "' AND o_w_id='" + w_id + "'");
		for (CqlRow row : result.getRows()) {
			customer_key = toString(row.getKey());
			break;
		}

		/* set carrier id for order */
		String order_key = w_id + "_" + d_id + "_" + no_o_id;
		executeQuery("UPDATE order SET o_carrier_id='" + o_carrier_id + "'  WHERE key='" + order_key  + "'");
		
		/* set deliver time for order line */
		String ol_delivery_d = (new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")).format(new Date(System.currentTimeMillis()));
		String ol_number = null, order_line_key = null;
		result = executeQuery("SELECT ol_number FROM order_line WHERE ol_w_id='" + w_id 
				+ "' AND ol_d_id='" + d_id + "' AND ol_o_id='" + no_o_id + "'");
		for (CqlRow row : result.getRows()) {
			for (Column column : row.getColumns() ) {
				switch(toString(column.getName())) {
				case "ol_number": 
					ol_number = toString(column.getValue());
					order_line_key = w_id + "_" + d_id + "_" + no_o_id + "_" + ol_number;
					executeQuery("UPDATE order_line SET ol_delivery_d='" + ol_delivery_d + "' WHERE key='" + order_line_key + "'");
					break;
				default: System.out.println("Error: Delivery - fail to retrieve order line information");
				}
			}
		}
		
		/* calculate the total amount for all items */
		float ol_total = 0;
		result = executeQuery("SELECT ol_amount FROM order_line WHERE ol_o_id='" + no_o_id 
				+ "' AND ol_d_id='" + d_id + "' AND ol_w_id='" + w_id + "'");
		for (CqlRow row : result.getRows()) {
			for (Column column : row.getColumns() ) {
				switch(toString(column.getName())) {
				case "ol_amount": 
					ol_total += Float.valueOf(toString(column.getValue())); 
					break;
				default: System.out.println("Error: Delivery - fail to retrieve ol_amount information");
				}
			}
		}
		
		/* retrieve balance of customers and c_delivery_cnt*/
		float c_balance =  0.0f;
		int c_delivery_cnt = 0;
		result = executeQuery("SELECT c_balance, c_delivery_cnt FROM customer WHERE key='" + customer_key + "'");
		for (CqlRow row : result.getRows()) {
			for (Column column : row.getColumns() ) {
				switch(toString(column.getName())) {
				case "c_balance":  c_balance = Float.valueOf(toString(column.getValue())); break;
				case "c_delivery_cnt":  c_delivery_cnt = Integer.valueOf(toString(column.getValue())); break;
				default: System.out.println("Error: Delivery - fail to retrieve ol_amount information");
				}
			}
			break;
		}

		/* update c_balance, c_delivery_cnt of customers */
		result = executeQuery("UPDATE customer SET c_balance='" + (c_balance + ol_total) 
				+ "', c_delivery_cnt='" + (c_delivery_cnt + 1) + "'  WHERE key='" + customer_key + "'");
		
		/* output */
		System.out.println("==============================Delivery==================================");
		System.out.println("INPUT	o_carrier_id: " + o_carrier_id);
		System.out.println();
		System.out.println("Warehouse: " + w_id);
		System.out.println("o_carrier_id: " + o_carrier_id);
		System.out.println("Execution Status: Delivery has been queued");
		System.out.println("=========================================================================");
		/* Close database connection */
		closeConnection();
	}
	
	
	/*
	 * Function name: Stocklevel
	 * Description: The Stock-Level business transaction determines the number of recently
	 * 				sold items that have a stock level below a specified threshold.
	 * Argument: threshold - randomInt(10, 20)
	 */
	public void Stocklevel(int threshold) throws InvalidRequestException, TException, UnsupportedEncodingException {
		
		/* Set up database connection */
		getConnection();
		
		/* local variables */
		int w_id = randomInt(1, COUNT_WARE);
		int d_id = randomInt(1, DIST_PER_WARE);
		CqlResult result;

		/* retrieve district d_next_o_id  */
		int d_next_o_id = 0;
		String district_key = w_id + "_" + d_id;
		result = executeQuery("SELECT d_next_o_id FROM district WHERE key='" + district_key + "'");
		for (CqlRow row : result.getRows()) {
			for (Column column : row.getColumns() ) {
				switch(toString(column.getName())) {
				case "d_next_o_id": d_next_o_id = Integer.valueOf(toString(column.getValue())); break;
				default: System.out.println("Error: Stocklevel - fail to retrieve district information");
				}
			}
		}
		
		/* retrieve order_line information  */
		int ol_i_id = 0;
		int low_stock = 0;
		/*
		 * All rows in the ORDER-LINE table with matching OL_W_ID (equals W_ID), OL_D_ID (equals D_ID), and
		 * OL_O_ID (lower than D_NEXT_O_ID and greater than or equal to D_NEXT_O_ID minus 20) are selected.
		 * They are the items for 20 recent orders of the district.
		 */
		result = executeQuery("SELECT ol_i_id FROM order_line "
				+ " WHERE ol_w_id='" + w_id  + "' AND ol_d_id='" + d_id 
				+  "' AND ol_o_id<'" + d_next_o_id  +  "' AND ol_o_id>='" + (d_next_o_id - 20) + "'");
		/*
		 * All rows in the STOCK table with matching S_I_ID (equals OL_I_ID) and S_W_ID (equals W_ID) from
		 * the list of distinct item numbers and with S_QUANTITY lower than threshold are counted (giving low_stock).
		 */
		Set<Integer> set = new HashSet<Integer>();
		for (CqlRow row : result.getRows()) {
			for (Column column : row.getColumns() ) {
				switch(toString(column.getName())) {
				case "ol_i_id": 
					ol_i_id = Integer.valueOf(toString(column.getValue())); 
					if (set.add(ol_i_id)) {
						CqlResult item = executeQuery("SELECT s_quantity FROM stock  WHERE s_i_id='" + ol_i_id 
								+ "' AND s_quantity<'" + threshold + "'");
						for (CqlRow item_row : item.getRows()) {
							for (Column item_column : item_row.getColumns() ) {
								low_stock += Integer.valueOf(toString(item_column.getValue()));
							}
						}
					} 
					break;
				default: System.out.println("Error: Stocklevel - fail to retrieve district information");
				}
			}
		}
		
		/* output */
		System.out.println("==============================Stock Level================================");
		System.out.println("INPUT	threshold: " + threshold);
		System.out.println();
		System.out.println("Warehouse: " + w_id + "\tDistrict: " + d_id);
		System.out.println("Stock Level Threshold: " + threshold);
		System.out.println("low stock: " + low_stock);
		System.out.println("=========================================================================");

		/* Close database connection */
		closeConnection();
	}
	
	
	public static String Lastname(int num) {
		String name = "";
		String n[] = {"BAR", "OUGHT", "ABLE", "PRI", "PRES", "ESE", "ANTI", "CALLY", "ATION", "EING"};
		name += n[num / 100];
		name += n[(num / 10) % 10];
		name += n[num % 10];
		return name;
	}
}
