import java.nio.ByteBuffer;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Random;

import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ColumnParent;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.InvalidRequestException;
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
public class Loader {

	/* Database Configuration */
	private String DB_KEYSPACE = "tpcc";
	private String DB_ADDRESS = "localhost";
	private int DB_PORT = 9160;
	/* db variables */
	private TTransport DB_TR = null;
	Cassandra.Client client = null;
	/* Number of warehouses */
	private int count_ware; 
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
	/* options */
	boolean option_debug; /* Debug option */
	/* Used as keys increment counter */
	int h_key = 1;
	
	/* 
	 * Description: Constructor for the class
	 * Argument: none
	 */
	public Loader() {
		this.count_ware = 2;
		option_debug = false;
	}
	
	/* 
	 * Description: Constructor for the class
	 * Argument: count_ware
	 */
	public Loader(int w) {
		this.count_ware = w;
	}
	
	/* 
	 * Description: Constructor for the class
	 * Arguments: count_ware, option_debug
	 */
	public Loader(int w, boolean d) {
		this.count_ware = w;
		option_debug = d;
	}
	
	public void start() throws InvalidRequestException, TException {
		System.out.println("============Start generating data for cassandra ============");
		this.LoadItems();
		this.LoadWare();
		this.LoadCust();
		this.LoadOrd();
	}
	
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

	/*
	 * This function randomly generates string,
	 * the string is mixed with letters and numbers
	 */
	public String MakeAlphaString(int min, int max) {
		StringBuffer str = new StringBuffer();
		Random random = new Random();
		long result = 0;
		int number = random.nextInt(max) % (max-min+1) + min;
		for (int i = 0; i < number; i++) {
			switch (random.nextInt(3)) {
			case 0: // CAP letter
				result = Math.round(Math.random() * 25 + 65);
				str.append(String.valueOf((char) result));
				break;
			case 1: // Low letter
				result = Math.round(Math.random() * 25 + 97);
				str.append(String.valueOf((char) result));
				break;
			case 2: // Number
				str.append(String.valueOf(new Random().nextInt(10)));
				break;
			}
		}
		return str.toString();
	}
	
	
	/*
	 * This function randomly generates string,
	 * the string is mixed with letters and numbers
	 */
	public String MakeNumberString(int min, int max) {
		StringBuffer str = new StringBuffer();
		Random random = new Random();
		int number = random.nextInt(max + 1)%(max-min+1) + min;
		for (int i = 0; i < number; i++) {
			str.append(String.valueOf(new Random().nextInt(10)));
		}
		return str.toString();
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

	/*
	 * Description: This function Load items into item table
	 */
	public void LoadItems() throws InvalidRequestException, TException {

		/* local varibales */
		int i_id;
		String i_name;
		float i_price;
		String i_data;

		int idatasiz;
		int orig[] = new int[MAXITEMS];
		int pos = 0;

		/* Set up database connection */
		getConnection();
		
		/* column family */
		ColumnParent column_parent = new ColumnParent(ITEM);
		
		/* start loading */
		System.out.println("Loading Item");

		/* random of 10% items that will be marked 'original ' */
		for (int i = 0; i < MAXITEMS; i++) {
			orig[i] = 0;
		}

		for (int i = 0; i < MAXITEMS / 10; i++) {
			do {
				pos = (new Random()).nextInt(MAXITEMS);
			} while (orig[pos] == 1);
			orig[pos] = 1;
		}

		for (i_id =1; i_id <=MAXITEMS; i_id++) {
			/* Generate Item Data */
			i_name = MakeAlphaString(14, 24);
			i_price = randomFloat(100, 10000) / 100.0f;
			i_data = MakeAlphaString(26, 50);
			idatasiz = i_data.length();
			
			if (orig[i_id - 1] == 1) {
				pos = randomInt(0, idatasiz - 8);
				i_data = i_data.substring(0, pos) + "original" + i_data.substring(pos+8);
			}
			
			if (option_debug) {
				System.out.println( "IID = " + i_id + ", Name= " + i_name + " , Price = " + i_price);
			}
			
			/* key */
			ByteBuffer key = ByteBuffer.wrap(String.valueOf(i_id).getBytes());
			/* column */
			Column column = new Column();
			column.setTimestamp(System.currentTimeMillis());
			column.setName(toByteBuffer("i_id"));
			column.setValue(toByteBuffer(i_id));
			client.insert(key, column_parent, column, ConsistencyLevel.ONE);
			column.setName(toByteBuffer("i_name"));
			column.setValue(toByteBuffer(i_name));
			client.insert(key, column_parent, column, ConsistencyLevel.ONE);
			column.setName(toByteBuffer("i_price"));
			column.setValue(toByteBuffer(i_price));
			client.insert(key, column_parent, column, ConsistencyLevel.ONE);
			column.setName(toByteBuffer("i_data"));
			column.setValue(toByteBuffer(i_data));
			client.insert(key, column_parent, column, ConsistencyLevel.ONE);
			
			if( i_id % 100 == 0 ){
				System.out.print(".");
				if ( i_id % 5000 == 0 ) System.out.println(i_id);
			}
		}
		
		System.out.println("Item Done");
		/* Close database connection */
		closeConnection();
	}

	/*
	 * Function name: LoadWare
	 * Description: Load the stock table, then call Stock and District
	 * Argument: none
	 */
	public void LoadWare() throws InvalidRequestException, TException {
		int w_id;
		String w_name;
		String w_street_1;
		String w_street_2; 
		String w_city; 
		String w_state; 
		String w_zip;
		float w_tax;
		float w_ytd;
		
		/* start loading */
		System.out.println("Loading Warehouses ...");
		
		for (w_id = 1; w_id <= count_ware; w_id++) {
			
			/* Set up database connection */
			getConnection();
			
			/* Generate Warehouse Data */
			w_name = MakeAlphaString( 6, 10 );
			w_street_1 = MakeAlphaString( 10,20); 	/* Street 1 */
			w_street_2 = MakeAlphaString( 10,20 ); 	/* Street 2 */ 
			w_city = MakeAlphaString( 10,20 ); 		/* City */
			w_state = MakeAlphaString( 2,2 ); 		/* State */
			w_zip = MakeNumberString( 9,9 ); 		/* Zip */
			w_tax= randomFloat( 10,20 ) / 100.0f;
			w_ytd = 3000000.0f;
			
			if ( option_debug ) {
				System.out.println( "WID = " + w_id + ", Name= " + w_name + ", Tax = " + w_tax);
			}
			
			/* key */
			ByteBuffer key = ByteBuffer.wrap(String.valueOf(w_id).getBytes());
			/* column family */
			ColumnParent column_parent = new ColumnParent(WAREHOUSE);
			/* column */
			Column column = new Column();
			column.setTimestamp(System.currentTimeMillis());
			column.setName(toByteBuffer("w_id"));
			column.setValue(toByteBuffer(w_id));
			client.insert(key, column_parent, column, ConsistencyLevel.ONE);
			column.setName(toByteBuffer("w_name"));
			column.setValue(toByteBuffer(w_name));
			client.insert(key, column_parent, column, ConsistencyLevel.ONE);
			column.setName(toByteBuffer("w_street_1"));
			column.setValue(toByteBuffer(w_street_1));
			client.insert(key, column_parent, column, ConsistencyLevel.ONE);
			column.setName(toByteBuffer("w_street_2"));
			column.setValue(toByteBuffer(w_street_2));
			client.insert(key, column_parent, column, ConsistencyLevel.ONE);
			column.setName(toByteBuffer("w_city"));
			column.setValue(toByteBuffer(w_city));
			client.insert(key, column_parent, column, ConsistencyLevel.ONE);
			column.setName(toByteBuffer("w_state"));
			column.setValue(toByteBuffer(w_state));
			client.insert(key, column_parent, column, ConsistencyLevel.ONE);
			column.setName(toByteBuffer("w_zip"));
			column.setValue(toByteBuffer(w_zip));
			client.insert(key, column_parent, column, ConsistencyLevel.ONE);
			column.setName(toByteBuffer("w_tax"));
			column.setValue(toByteBuffer(w_tax));
			client.insert(key, column_parent, column, ConsistencyLevel.ONE);
			column.setName(toByteBuffer("w_ytd"));
			column.setValue(toByteBuffer(w_ytd));
			client.insert(key, column_parent, column, ConsistencyLevel.ONE);
			
			/* Make rows associated with warehouse*/
			Stock(w_id);
			District(w_id);
			
			/* Close database connection */
			closeConnection();
		}
		
	}
	
	/*
	 * Function name: Stock
	 * Description: Load the stock table
	 * Argument: w_id - warehouse id
	 */
	void Stock(int w_id) throws InvalidRequestException, TException {
		
		/* local varibales */
		int s_i_id;
		int s_w_id;
		int s_quantity;
		String s_data;
		
		int sdatasiz;
		int orig[] = new int[MAXITEMS];
		int pos;
		
		/* Set up database connection */
		getConnection();
		
		/* column family */
		ColumnParent column_parent = new ColumnParent(STOCK);
		
		/* Starting Loading ...*/
		System.out.println("Loading Stock Wid = " + w_id);
		s_w_id = w_id;
		
		for (int i=0; i < MAXITEMS; i++) {
			orig[i] = 0; 
		}
		
		for (int i = 0; i < MAXITEMS / 10; i++)
		{
			do {
				pos = (new Random()).nextInt(MAXITEMS);
			} while(orig[pos] == 1);
			orig[pos] = 1;
		}
		
		for (s_i_id = 1; s_i_id <= MAXITEMS; s_i_id++) {
			/* Generate Stock Data */
			s_quantity= randomInt(10,100);
			s_data = MakeAlphaString(26,50);
			sdatasiz = s_data.length();
			if (orig[s_i_id-1] == 1) { 
				pos = randomInt(0, sdatasiz - 8); 
				s_data = s_data.substring(0, pos) + "original" + s_data.substring(pos+8);
			}
			
			/* key */
			ByteBuffer key = toByteBuffer(s_w_id + "_" + s_i_id);
			/* column */
			Column column = new Column();
			column.setTimestamp(System.currentTimeMillis());
			column.setName(toByteBuffer("s_i_id"));
			column.setValue(toByteBuffer(s_i_id));
			client.insert(key, column_parent, column, ConsistencyLevel.ONE);
			column.setName(toByteBuffer("s_w_id"));
			column.setValue(toByteBuffer(s_w_id));
			client.insert(key, column_parent, column, ConsistencyLevel.ONE);
			column.setName(toByteBuffer("s_quantity"));
			column.setValue(toByteBuffer(s_quantity));
			client.insert(key, column_parent, column, ConsistencyLevel.ONE);
			/* S_DIST_01 ... S_DIST_10*/
			for (int i = 1; i <= DIST_PER_WARE - 1; i++) {
				column.setName((toByteBuffer("s_dist_0" + String.valueOf(i))));
				column.setValue(toByteBuffer(MakeAlphaString(24,24)));
				client.insert(key, column_parent, column, ConsistencyLevel.ONE);
			}
			column.setName((toByteBuffer("s_dist_10")));
			column.setValue(toByteBuffer(MakeAlphaString(24,24)));
			client.insert(key, column_parent, column, ConsistencyLevel.ONE);
			column.setName(toByteBuffer("s_data"));
			column.setValue(toByteBuffer(s_data));
			client.insert(key, column_parent, column, ConsistencyLevel.ONE);
			column.setName(toByteBuffer("s_ytd"));
			column.setValue("0".getBytes());
			client.insert(key, column_parent, column, ConsistencyLevel.ONE);
			column.setName(toByteBuffer("s_order_cnt"));
			column.setValue("0".getBytes());
			client.insert(key, column_parent, column, ConsistencyLevel.ONE);
			column.setName(toByteBuffer("s_remote_cnt"));
			column.setValue("0".getBytes());
			client.insert(key, column_parent, column, ConsistencyLevel.ONE);
			
			if ( option_debug ) {
				System.out.println( "SID = " + s_i_id + ", WID = " + s_w_id + ", Quan = " + s_quantity );
			}
			
			if ( (s_i_id % 100) == 0 ) {
				System.out.print(".");
				if ( s_i_id % 5000 == 0) System.out.println(s_i_id);
			}
		}
		
		System.out.println("Stock Done Wid = " + w_id);
		
		/* Close database connection */
		closeConnection();
	}

	/*
	 * Function name: District
	 * Description: Load the district table
	 * Argument: w_id - warehouse id
	 */
	void District(int w_id) throws InvalidRequestException, TException {
		/* local varibales */
		int d_id;
		int d_w_id;
		String d_name;
		String d_street_1;
		String d_street_2; 
		String d_city; 
		String d_state; 
		String d_zip;
		float d_tax;
		float d_ytd;
		int d_next_o_id;
		
		/* Set up database connection */
		getConnection();
		
		/* column family */
		ColumnParent column_parent = new ColumnParent(DISTRICT);
		
		/* Starting Loading ...*/
		System.out.println("Loading District Wid = " + w_id);
		
		d_w_id = w_id; 
		d_ytd = 30000.0f; 
		d_next_o_id = 3001;
		
		for (d_id = 1; d_id <= DIST_PER_WARE; d_id++) {
			/* Generate District Data */ 
			d_name = MakeAlphaString(6,10);
			d_street_1 = MakeAlphaString( 10,20); 	/* Street 1 */
			d_street_2 = MakeAlphaString( 10,20 ); 	/* Street 2 */ 
			d_city = MakeAlphaString( 10,20 ); 		/* City */
			d_state = MakeAlphaString( 2,2 ); 		/* State */
			d_zip = MakeNumberString( 9,9 ); 		/*Zip */
			d_tax=(randomFloat(10,20)) / 100.0f;
			
			if ( option_debug ) {
				System.out.println( "DID = " + d_id +", WID = " + d_w_id + ", Name = " + d_name + ", Tax = " + d_tax );
			}
			
			/* key */
			ByteBuffer key = toByteBuffer(d_w_id + "_" + d_id);
			/* column */
			Column column = new Column();
			column.setTimestamp(System.currentTimeMillis());
			column.setName(toByteBuffer("d_id"));
			column.setValue(toByteBuffer(d_id));
			client.insert(key, column_parent, column, ConsistencyLevel.ONE);
			column.setName(toByteBuffer("d_w_id"));
			column.setValue(toByteBuffer(d_w_id));
			client.insert(key, column_parent, column, ConsistencyLevel.ONE);
			column.setName(toByteBuffer("d_name"));
			column.setValue(toByteBuffer(d_name));
			client.insert(key, column_parent, column, ConsistencyLevel.ONE);
			column.setName(toByteBuffer("d_street_1"));
			column.setValue(toByteBuffer(d_street_1));
			client.insert(key, column_parent, column, ConsistencyLevel.ONE);
			column.setName(toByteBuffer("d_street_2"));
			column.setValue(toByteBuffer(d_street_2));
			client.insert(key, column_parent, column, ConsistencyLevel.ONE);
			column.setName(toByteBuffer("d_city"));
			column.setValue(toByteBuffer(d_city));
			client.insert(key, column_parent, column, ConsistencyLevel.ONE);
			column.setName(toByteBuffer("d_state"));
			column.setValue(toByteBuffer(d_state));
			client.insert(key, column_parent, column, ConsistencyLevel.ONE);
			column.setName(toByteBuffer("d_zip"));
			column.setValue(toByteBuffer(d_zip));
			client.insert(key, column_parent, column, ConsistencyLevel.ONE);
			column.setName(toByteBuffer("d_tax"));
			column.setValue(toByteBuffer(d_tax));
			client.insert(key, column_parent, column, ConsistencyLevel.ONE);
			column.setName(toByteBuffer("d_ytd"));
			column.setValue(toByteBuffer(d_ytd));
			client.insert(key, column_parent, column, ConsistencyLevel.ONE);
			column.setName(toByteBuffer("d_next_o_id"));
			column.setValue(toByteBuffer(d_next_o_id));
			client.insert(key, column_parent, column, ConsistencyLevel.ONE);
		}
		
		System.out.println("District Done");
		
		/* Close database connection */
		closeConnection();
	}
	
	/*
	 * Function name: LoadCust
	 * Description: Call Customer() to load the Customer table
	 * Argument: none
	 */
	public void LoadCust() throws InvalidRequestException, TException {

		/* Set up database connection */
		getConnection();
		
		for (int w_id = 1; w_id<=count_ware; w_id++) {
			for (int d_id = 1; d_id<=DIST_PER_WARE; d_id++) {
				Customer(d_id, w_id);
			}
		}
		
		/* Close database connection */
		closeConnection();

	}

	/*
	 * Function name: LoadCust
	 * Description: Load the customer table, the histroy table
	 * Argument: none
	 */
	void Customer(int d_id, int w_id) throws InvalidRequestException, UnavailableException, TimedOutException, TException {
		int	c_id, c_d_id, c_w_id, c_credit_lim;
		String c_first, c_middle, c_last, c_street_1, c_street_2, c_city, c_state, c_zip;
		String c_phone, c_since, c_credit, c_data, h_date, h_data; 
		float c_discount, c_balance, h_amount; 
		
		/* Already Set up database connection */
		System.out.println("Loading Customer for DID="+ d_id +", WID=" + w_id);
		
		/* column family */
		ColumnParent column_parent_c = new ColumnParent(CUSTOMER);
		ColumnParent column_parent_h = new ColumnParent(HISTORY);
		
		for (c_id = 1; c_id <= CUST_PER_DIST; c_id ++) {
			/* Generate Customer Data */
			c_d_id = d_id;
			c_w_id = w_id;
			c_first = MakeAlphaString( 8, 16 ); 
			c_middle = "OE";
			if (c_id <= 1000) { 
				c_last = Lastname(c_id - 1);
			} else {
				c_last = Lastname(NURand(A_C_LAST, 0, 999));
			}
			c_street_1 = MakeAlphaString( 10,20); 	/* Street 1 */
			c_street_2 = MakeAlphaString( 10,20 ); 	/* Street 2 */ 
			c_city = MakeAlphaString( 10,20 ); 		/* City */
			c_state = MakeAlphaString( 2,2 ); 		/* State */
			c_zip = MakeNumberString( 9,9 ); 		/* Zip */
			c_since = (new SimpleDateFormat("yyyy-MM-dd")).format(new Date(System.currentTimeMillis()));
			c_phone = MakeNumberString(16, 16);
			if ( randomInt( 0, 1 ) == 1) {
				c_credit = "GC";
			} else {
				c_credit = "BC";
			}
			c_credit_lim = 50000;
			c_discount = (randomFloat(0, 50)) / 100.0f;
			c_balance = -10.0f;
			c_data = MakeAlphaString(300, 500);
			
			/* Insert into database */
			
			/* key */
			ByteBuffer key = toByteBuffer(c_w_id + "_" +  c_d_id + "_" + c_id);
			/* insert into the customer table */
			Column column = new Column();
			column.setTimestamp(System.currentTimeMillis());
			column.setName(toByteBuffer("c_id"));
			column.setValue(toByteBuffer(c_id));
			client.insert(key, column_parent_c, column, ConsistencyLevel.ONE);
			column.setName(toByteBuffer("c_d_id"));
			column.setValue(toByteBuffer(c_d_id));
			client.insert(key, column_parent_c, column, ConsistencyLevel.ONE);
			column.setName(toByteBuffer("c_w_id"));
			column.setValue(toByteBuffer(c_w_id));
			client.insert(key, column_parent_c, column, ConsistencyLevel.ONE);
			column.setName(toByteBuffer("c_first"));
			column.setValue(toByteBuffer(c_first));
			client.insert(key, column_parent_c, column, ConsistencyLevel.ONE);
			column.setName(toByteBuffer("c_middle"));
			column.setValue(toByteBuffer(c_middle));
			client.insert(key, column_parent_c, column, ConsistencyLevel.ONE);
			column.setName(toByteBuffer("c_last"));
			column.setValue(toByteBuffer(c_last));
			client.insert(key, column_parent_c, column, ConsistencyLevel.ONE);
			column.setName(toByteBuffer("c_street_1"));
			column.setValue(toByteBuffer(c_street_1));
			client.insert(key, column_parent_c, column, ConsistencyLevel.ONE);
			column.setName(toByteBuffer("c_street_2"));
			column.setValue(toByteBuffer(c_street_2));
			client.insert(key, column_parent_c, column, ConsistencyLevel.ONE);
			column.setName(toByteBuffer("c_city"));
			column.setValue(toByteBuffer(c_city));
			client.insert(key, column_parent_c, column, ConsistencyLevel.ONE);
			column.setName(toByteBuffer("c_state"));
			column.setValue(toByteBuffer(c_state));
			client.insert(key, column_parent_c, column, ConsistencyLevel.ONE);
			column.setName(toByteBuffer("c_zip"));
			column.setValue(toByteBuffer(c_zip));
			client.insert(key, column_parent_c, column, ConsistencyLevel.ONE);
			column.setName(toByteBuffer("c_phone"));
			column.setValue(toByteBuffer(c_phone));
			client.insert(key, column_parent_c, column, ConsistencyLevel.ONE);
			column.setName(toByteBuffer("c_since"));
			column.setValue(toByteBuffer(c_since));
			client.insert(key, column_parent_c, column, ConsistencyLevel.ONE);
			column.setName(toByteBuffer("c_credit"));
			column.setValue(toByteBuffer(c_credit));
			client.insert(key, column_parent_c, column, ConsistencyLevel.ONE);
			column.setName(toByteBuffer("c_credit_lim"));
			column.setValue(toByteBuffer(c_credit_lim));
			client.insert(key, column_parent_c, column, ConsistencyLevel.ONE);
			column.setName(toByteBuffer("c_discount"));
			column.setValue(toByteBuffer(c_discount));
			client.insert(key, column_parent_c, column, ConsistencyLevel.ONE);
			column.setName(toByteBuffer("c_balance"));
			column.setValue(toByteBuffer(c_balance));
			client.insert(key, column_parent_c, column, ConsistencyLevel.ONE);
			column.setName(toByteBuffer("c_ytd_payment"));
			column.setValue(toByteBuffer(10.0f));
			client.insert(key, column_parent_c, column, ConsistencyLevel.ONE);
			column.setName(toByteBuffer("c_payment_cnt"));
			column.setValue(toByteBuffer(1));
			client.insert(key, column_parent_c, column, ConsistencyLevel.ONE);
			column.setName(toByteBuffer("c_deliver_cnt"));
			column.setValue(toByteBuffer(0));
			client.insert(key, column_parent_c, column, ConsistencyLevel.ONE);
			column.setName(toByteBuffer("c_data"));
			column.setValue(toByteBuffer(c_data));
			client.insert(key, column_parent_c, column, ConsistencyLevel.ONE);
			
			/* history table data generation */
			h_amount = 10.0f; 
			h_date = (new SimpleDateFormat("yyyy-MM-dd")).format(new Date(System.currentTimeMillis()));
			h_data = MakeAlphaString( 12,24 );
			
			/* key */
			key = ByteBuffer.wrap(String.valueOf(System.currentTimeMillis()).getBytes());
			/* insert into the customer table */
			column = new Column();
			column.setTimestamp(System.currentTimeMillis());
			column.setName(toByteBuffer("h_c_id"));
			column.setValue(toByteBuffer(c_id));
			client.insert(key, column_parent_h, column, ConsistencyLevel.ONE);
			column.setName(toByteBuffer("h_c_d_id"));
			column.setValue(toByteBuffer(c_d_id));
			client.insert(key, column_parent_h, column, ConsistencyLevel.ONE);
			column.setName(toByteBuffer("h_c_w_id"));
			column.setValue(toByteBuffer(c_w_id));
			client.insert(key, column_parent_h, column, ConsistencyLevel.ONE);
			column.setName(toByteBuffer("h_w_id"));
			column.setValue(toByteBuffer(c_w_id));
			client.insert(key, column_parent_h, column, ConsistencyLevel.ONE);
			column.setName(toByteBuffer("h_d_id"));
			column.setValue(toByteBuffer(c_d_id));
			client.insert(key, column_parent_h, column, ConsistencyLevel.ONE);
			column.setName(toByteBuffer("h_date"));
			column.setValue(toByteBuffer(h_date));
			client.insert(key, column_parent_h, column, ConsistencyLevel.ONE);
			column.setName(toByteBuffer("h_amount"));
			column.setValue(toByteBuffer(h_amount));
			client.insert(key, column_parent_h, column, ConsistencyLevel.ONE);
			column.setName(toByteBuffer("h_data"));
			column.setValue(toByteBuffer(h_data));
			client.insert(key, column_parent_h, column, ConsistencyLevel.ONE);
			
			if (option_debug) {
				System.out.println("CID = " + c_id + ", LST = " + c_last + ", P# = " + c_phone);
			}
			if (c_id % 100 == 0) {
				System.out.print(".");
				if (c_id % 1000 == 0) System.out.println(c_id);
			}
		}
		
		System.out.println("Customer Done.");

	}
	
	/*
	 * Function name: LoadOrd
	 * Description: Call Orders() to load the order table and new_order table
	 * Argument: none
	 */
	public void LoadOrd() throws InvalidRequestException, TException {

		/* Set up database connection */
		getConnection();
		
		for (int w_id = 1; w_id<=count_ware; w_id++) {
			for (int d_id = 1; d_id<=DIST_PER_WARE; d_id++) {
				Orders(d_id, w_id);
			}
		}
		
		/* Close database connection */
		closeConnection();
	}

	/*
	 * Function name: Orders
	 * Description: Loads the order table as well as the order_line table
	 * Argument: d_id - district id
	 * 			 w_id - warehouse id
	 */
	void Orders( int d_id, int w_id ) throws InvalidRequestException, UnavailableException, TimedOutException, TException {
		int o_id, o_c_id, o_d_id, o_w_id, o_carrier_id, o_ol_cnt;
		int ol, ol_i_id, ol_supply_w_id, ol_quantity;
		String o_entry_d, ol_dist_info, ol_delivery_d; 
		float ol_amount;
		
		System.out.println("Loading Orders for D=" + d_id + ", W=" + w_id); 
		
		o_d_id = d_id;
		o_w_id = w_id;
		o_entry_d = (new SimpleDateFormat("yyyy-MM-dd")).format(new Date(System.currentTimeMillis()));
		ol_delivery_d = (new SimpleDateFormat("yyyy-MM-dd")).format(new Date(System.currentTimeMillis()));
		/* initialize permutation of customer numbers */
		int permutation[] = new int[CUST_PER_DIST];
		for (int i = 0; i < CUST_PER_DIST; i++) {
			permutation[i] = i + 1;
		}
		for (int i = 0; i < permutation.length; i++) {
			int index = (new Random()).nextInt(CUST_PER_DIST);
			int t = permutation[i];
			permutation[i] = permutation[index];
			permutation[index] = t;
		}
		
		/* column family */
		ColumnParent column_parent_o = new ColumnParent(ORDER);
		ColumnParent column_parent_ol = new ColumnParent(ORDERLINE);
		
		for (o_id = 1; o_id <= ORD_PER_DIST; o_id++) {
			/* Generate Order Data */ 
			o_c_id = permutation[o_id - 1];
			o_carrier_id = randomInt( 1, 10 );
			o_ol_cnt = randomInt( 5, 15 );
			
			/* key */
			ByteBuffer key = toByteBuffer(o_w_id + "_" + o_d_id + "_" + o_id);
			/* insert into the customer table */
			Column column = new Column();
			column.setTimestamp(System.currentTimeMillis());
			column.setName(toByteBuffer("o_id"));
			column.setValue(toByteBuffer(o_id));
			client.insert(key, column_parent_o, column, ConsistencyLevel.ONE);
			column.setName(toByteBuffer("o_d_id"));
			column.setValue(toByteBuffer(o_d_id));
			client.insert(key, column_parent_o, column, ConsistencyLevel.ONE);
			column.setName(toByteBuffer("o_w_id"));
			column.setValue(toByteBuffer(o_w_id));
			client.insert(key, column_parent_o, column, ConsistencyLevel.ONE);
			column.setName(toByteBuffer("o_c_id"));
			column.setValue(toByteBuffer(o_c_id));
			client.insert(key, column_parent_o, column, ConsistencyLevel.ONE);
			column.setName(toByteBuffer("o_entry_d"));
			column.setValue(toByteBuffer(o_entry_d));
			client.insert(key, column_parent_o, column, ConsistencyLevel.ONE);
			column.setName(toByteBuffer("o_ol_cnt"));
			column.setValue(toByteBuffer(o_ol_cnt));
			client.insert(key, column_parent_o, column, ConsistencyLevel.ONE);
			column.setName(toByteBuffer("o_all_local"));
			column.setValue(toByteBuffer(1));
			client.insert(key, column_parent_o, column, ConsistencyLevel.ONE);
			
			if (o_id > 2100) { 
				/* the last 900 orders have not been delivered */
				column.setName(toByteBuffer("o_carrier_id"));
				column.setValue(toByteBuffer("NULL"));
				client.insert(key, column_parent_o, column, ConsistencyLevel.ONE);
				
				/* Load new order table */
				New_Orders( o_id, o_w_id, o_d_id );
			} else {
				/* the first 2100 orders have not been delivered */
				column.setName(toByteBuffer("o_carrier_id"));
				column.setValue(toByteBuffer(o_carrier_id));
				client.insert(key, column_parent_o, column, ConsistencyLevel.ONE);
			}
			
			for (ol = 1; ol <= o_ol_cnt; ol++) {
				
				/* Generate Order Line Data */
				ol_i_id = randomInt(1, MAXITEMS);
				ol_supply_w_id = o_w_id;
				ol_quantity = 5;
				ol_amount = randomFloat(10, 10000) / 100.0f; /* randomly generated in specification */
				ol_dist_info = MakeAlphaString( 24,24 );
				
				key = toByteBuffer(w_id + "_" + d_id + "_" + o_id + "_" + ol);
				column.setName(toByteBuffer("ol_o_id"));
				column.setValue(toByteBuffer(o_id));
				client.insert(key, column_parent_ol, column, ConsistencyLevel.ONE);
				column.setName(toByteBuffer("ol_d_id"));
				column.setValue(toByteBuffer(o_d_id));
				client.insert(key, column_parent_ol, column, ConsistencyLevel.ONE);
				column.setName(toByteBuffer("ol_w_id"));
				column.setValue(toByteBuffer(o_w_id));
				client.insert(key, column_parent_ol, column, ConsistencyLevel.ONE);
				column.setName(toByteBuffer("ol_number"));
				column.setValue(toByteBuffer(ol));
				client.insert(key, column_parent_ol, column, ConsistencyLevel.ONE);
				column.setName(toByteBuffer("ol_i_id"));
				column.setValue(toByteBuffer(ol_i_id));
				client.insert(key, column_parent_ol, column, ConsistencyLevel.ONE);
				column.setName(toByteBuffer("ol_supply_w_id"));
				column.setValue(toByteBuffer(ol_supply_w_id));
				client.insert(key, column_parent_ol, column, ConsistencyLevel.ONE);
				column.setName(toByteBuffer("ol_quantity"));
				column.setValue(toByteBuffer(ol_quantity));
				client.insert(key, column_parent_ol, column, ConsistencyLevel.ONE);
				column.setName(toByteBuffer("ol_dist_info"));
				column.setValue(toByteBuffer(ol_dist_info));
				client.insert(key, column_parent_ol, column, ConsistencyLevel.ONE);
				column.setName(toByteBuffer("ol_amount"));
				column.setValue(toByteBuffer(ol_amount));
				client.insert(key, column_parent_ol, column, ConsistencyLevel.ONE);
				
				if (o_id > 2100) {
					column.setName(toByteBuffer("ol_delivery_d"));
					column.setValue(toByteBuffer("NULL"));
					client.insert(key, column_parent_ol, column, ConsistencyLevel.ONE);
					
				} else {
					column.setName(toByteBuffer("ol_delivery_d"));
					column.setValue(toByteBuffer(ol_delivery_d));
					client.insert(key, column_parent_ol, column, ConsistencyLevel.ONE);
				}
				
				if ( option_debug ) {
					System.out.println( "OL = " + ol + ", IID = " + ol_i_id +", QUAN = " + ol_quantity + " , AMT = " + ol_amount );
				}
			}
			
			if ( o_id % 100 == 0 ){
				System.out.print(".");
				if ( o_id % 1000 == 0 ) System.out.println( o_id );
			}
		}
		
		System.out.println( "Orders Done." ) ;
	}
	
	/*
	 * Function name: New_Orders
	 * Description:
	 * Argument: none
	 */
	void New_Orders(int o_id, int no_w_id, int no_d_id) throws InvalidRequestException, UnavailableException, TimedOutException, TException {
		/* database connection has already been set up */
		ColumnParent column_parent_no = new ColumnParent(NEWORDER);
		ByteBuffer key = toByteBuffer(no_w_id + "_" + no_d_id + "_" + o_id);
		Column column = new Column();
		column.setTimestamp(System.currentTimeMillis());
		
		column.setName(toByteBuffer("no_o_id"));
		column.setValue(toByteBuffer(o_id));
		client.insert(key, column_parent_no, column, ConsistencyLevel.ONE);
		column.setName(toByteBuffer("no_w_id"));
		column.setValue(toByteBuffer(no_w_id));
		client.insert(key, column_parent_no, column, ConsistencyLevel.ONE);
		column.setName(toByteBuffer("no_d_id"));
		column.setValue(toByteBuffer(no_d_id));
		client.insert(key, column_parent_no, column, ConsistencyLevel.ONE);
	}

	/*
	 * This function generates the last name for customers
	 * Argument : int num, String name
	 */
	public static String Lastname(int num) {
		String name = "";
		String n[] = {"BAR", "OUGHT", "ABLE", "PRI", "PRES", "ESE", "ANTI", "CALLY", "ATION", "EING"};
		name += n[num / 100];
		name += n[(num / 10) % 10];
		name += n[num % 10];
		return name;
	}

}