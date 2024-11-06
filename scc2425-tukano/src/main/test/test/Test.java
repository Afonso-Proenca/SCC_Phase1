package test;

import tukano.api.Result;
import tukano.api.User;
import tukano.clients.rest.RestBlobsClient;
import tukano.clients.rest.RestShortsClient;
import tukano.clients.rest.RestUsersClient;

import java.io.File;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.Random;

public class Test {
	
	static {
		System.setProperty("java.util.logging.SimpleFormatter.format", "%4$s: %5$s");
	}
	
	public static void main(String[] args ) throws Exception {
		/*new Thread( () -> {
			try { 
				TukanoRestServer.main( new String[] {} );
			} catch( Exception x ) {
				x.printStackTrace();
			}
		}).start();*/

		Thread.sleep(1000);
		//---
		//var serverURI = String.format("http://localhost:%s/rest", TukanoRestServer.PORT);
		var serverURI = "http://127.0.0.1:8080/insert/rest/";
		
		var blobs = new RestBlobsClient(serverURI);
		var users = new RestUsersClient( serverURI);
		var shorts = new RestShortsClient(serverURI);
				
		 show(users.createUser( new User("wales", "12345", "jimmy@wikipedia.pt", "Jimmy Wales") ));
		 //show(users.getUser("wales", "12345"));
		 //show(users.getUser("wales", "12345"));
		 
		 show(users.createUser( new User("liskov", "54321", "liskov@mit.edu", "Barbara Liskov") ));
		 
		 //show(users.updateUser("wales", "12345", new User("wales", "12345", "jimmy@wikipedia.com", "" ) ));
		 
		 
		//show(users.searchUsers(""));
		
		
		Result<tukano.api.Short> s1, s2;

		show(s2 = shorts.createShort("liskov", "54321"));		
		show(s1 = shorts.createShort("wales", "12345"));
		show(shorts.createShort("wales", "12345"));
		show(shorts.createShort("wales", "12345"));
		show(shorts.createShort("wales", "12345"));

		var blobUrl = URI.create(s2.value().getBlobUrl());
		System.out.println( "------->" + blobUrl );
		
		var blobId = new File( blobUrl.getPath() ).getName();
		System.out.println( "BlobID:" + blobId );

		var token = blobUrl.getQuery().split("=")[1];
		System.out.println("Token: " + token);
		
		blobs.upload(blobUrl.toString(), randomBytes( 100 ), token);

		
		var s2id = s2.value().getid();

		show(shorts.follow("liskov", "wales", true, "54321"));
		show(shorts.follow("wales", "liskov", true, "12345"));
		show(shorts.like(s2id, "wales", true, "12345"));
		show(shorts.like(s1.value().id(), "liskov", true, "54321"));
		show(shorts.deleteAllShorts("wales", "12345", ""));

		/*show(shorts.follow("liskov", "wales", true, "54321"));
		show(shorts.getFeed("liskov", "54321"));
		show(shorts.deleteShort(s1.value().id(), "12345"));
		show(shorts.getFeed("liskov", "54321"));
		show(shorts.follow("liskov", "wales", false, "54321"));
		show(shorts.getFeed("liskov", "54321"));
		show(shorts.follow("liskov", "wales", true, "54321"));
		show(shorts.createShort("wales", "12345"));
		show(shorts.createShort("wales", "12345"));
		show(shorts.createShort("wales", "12345"));
		show(shorts.createShort("wales", "12345"));
		show(shorts.getFeed("liskov", "54321"));
		/*show(shorts.follow("liskov", "wales", true, "54321"));
		show(shorts.followers("wales", "12345"));
		show(shorts.followers("wales", "12345"));
		show(shorts.follow("liskov", "wales", false, "54321"));
		show(shorts.followers("wales", "12345"));
		show(shorts.follow("liskov", "wales", true, "54321"));
		show(shorts.followers("wales", "12345"));
		show(shorts.followers("wales", "12345"));*/


		//show(shorts.getShort( s2id ));
		//show(shorts.getShort( s2id ));
		/*show(shorts.like(s2id, "liskov", true, "54321"));
		show(shorts.like(s2id, "liskov", true, "54321"));
		show(shorts.like(s2id, "wales", true, "12345"));
		show(shorts.getShort( s2id ));
		show(shorts.likes(s2id , "54321"));
		show(shorts.likes(s2id , "54321"));
		show(shorts.like(s2id, "liskov", false, "54321"));
		show(shorts.getShort( s2id ));
		show(shorts.likes(s2id , "54321"));
		show(shorts.like(s2id, "liskov", true, "54321"));
		show(shorts.likes(s2id , "54321"));
		show(shorts.likes(s2id , "54321"));*/
		//show(shorts.deleteShort(s2id, "54321"));
		//show(shorts.getFeed("liskov", "54321"));
		//show(shorts.getShort( s2id ));
		
		//show(shorts.getShorts( "wales" ));
		//show(shorts.getShorts( "wales" ));

		//show(shorts.followers("wales", "12345"));

		//show(shorts.getFeed("liskov", "12345"));

		//show(shorts.getShort( s2id ));
//
//		
//		blobs.forEach( b -> {
//			var r = b.download(blobId);
//			System.out.println( Hex.of(Hash.sha256( bytes )) + "-->" + Hex.of(Hash.sha256( r.value() )));
//			
//		});
		
		//show(users.deleteUser("wales", "12345"));

		System.exit(0);
	}
	
	
	private static Result<?> show( Result<?> res ) {
		if( res.isOK() )
			System.err.println("OK: " + res.value() );
		else
			System.err.println("ERROR:" + res.error());
		return res;
		
	}
	
	private static byte[] randomBytes(int size) {
		var r = new Random(1L);

		var bb = ByteBuffer.allocate(size);
		
		r.ints(size).forEach( i -> bb.put( (byte)(i & 0xFF)));		

		return bb.array();
		
	}
}
