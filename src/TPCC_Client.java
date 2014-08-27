import java.io.UnsupportedEncodingException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Random;

import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.NotFoundException;
import org.apache.cassandra.thrift.TimedOutException;
import org.apache.cassandra.thrift.UnavailableException;
import org.apache.thrift.TException;

/* 
 * TPC-C Transactions on Cassandra
 * @author Wei HU
 * @date 2014-08-25 
 */
public class TPCC_Client {

	/*
	 * Function name: main
	 * Description: This is the main driver
	 */
	public static void main(String[] args) throws TException,
			InvalidRequestException, UnavailableException,
			UnsupportedEncodingException, NotFoundException, TimedOutException {
		TPCC t = new TPCC();
		//t.Payment(1, "BAROUGHTABLE", 1, 1);;
		t.Delivery(5);
		
		//Loader l = new Loader();
		//l.start();
	}
}
