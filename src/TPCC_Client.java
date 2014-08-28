import java.io.UnsupportedEncodingException;
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

		// 45 43 4 4 4
		// 0-44 45-87 88-91 92-95 96-99
		TPCC t = new TPCC();
		Random r = new Random();
		int x, y;
		long systime_start = System.currentTimeMillis();
		for (int i = 0; i < 1000; i++)
		{
			x = r.nextInt(100);
			y = r.nextInt(100);
			if (x <= 44) {
				t.Neworder(randomInt(1, TPCC.COUNT_WARE), randomInt(1, 10));
			} else if (x <= 87) {
				if (y < 60) {
					t.Payment(randomInt(1, TPCC.COUNT_WARE), randomInt(1, 10), Loader.Lastname(Loader.NURand(255,0,999)));
				} else {
					t.Payment(randomInt(1, TPCC.COUNT_WARE), randomInt(1, 10), Loader.NURand(1023,1,3000));
				}
				
			} else if (x <= 91) {
				if (y < 60){
					t.Orderstatus(randomInt(1, 10), Loader.Lastname(Loader.NURand(255,0,999)));
				} else {
					t.Orderstatus(randomInt(1, 10), Loader.NURand(1023,1,3000));
				}
				
			} else if (x <= 95) {
				t.Delivery(randomInt(1, 10));
			} else {
				t.Stocklevel(randomInt(10, 20));
			}
		}
		long systime_end = System.currentTimeMillis();
		System.out.println(systime_end - systime_start);
	}
	
	public static int randomInt(int min, int max) {
		return new Random().nextInt(max + 1) % (max - min + 1) + min;
	}
}
