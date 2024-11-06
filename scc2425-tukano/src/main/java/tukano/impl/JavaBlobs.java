package tukano.impl;

import static java.lang.String.format;
import static tukano.api.Result.error;
import static tukano.api.Result.ErrorCode.FORBIDDEN;
import static tukano.api.Result.ErrorCode.CONFLICT;
import static tukano.api.Result.ErrorCode.INTERNAL_ERROR;
import static tukano.api.Result.ErrorCode.NOT_FOUND;
import static tukano.api.Result.ok;

import java.util.List;
import java.util.logging.Logger;


import com.azure.core.util.BinaryData;
import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobContainerClientBuilder;

import cache.RedisCache;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisException;
import tukano.api.Blobs;
import tukano.api.Result;

import tukano.impl.rest.TukanoRestServer;
import utils.Hash;
import utils.Hex;
import utils.JSON;

public class JavaBlobs implements Blobs {
	
	private static Blobs instance;
	private static final Logger Log = Logger.getLogger(JavaBlobs.class.getName());
	private static final String BlobStoreConnection = System.getProperty("BlobStoreConnection");

	public String baseURI;
	private final BlobContainerClient containerClient;

	synchronized public static Blobs getInstance() {
		if( instance == null )
			instance = new JavaBlobs();
		return instance;
	}
	
	private JavaBlobs() {
		baseURI = format("%s/%s/", TukanoRestServer.serverURI, Blobs.NAME);

		containerClient = new BlobContainerClientBuilder()
				.connectionString(BlobStoreConnection)
				.containerName(Blobs.NAME)
				.buildClient();
	}
	
	@Override
public Result<Void> upload(String blobId, byte[] data, String token) {


    if (!isBlobIdValid(blobId, token)) {
        return error(FORBIDDEN);
    }

    try {
        BlobClient bc = containerClient.getBlobClient(blobId);
        BinaryData bdata = BinaryData.fromBytes(data);

        if (bc.exists()) {
            byte[] currentData = bc.downloadContent().toBytes();

            if (Hex.of(Hash.sha256(currentData)).equals(Hex.of(Hash.sha256(data)))) {
              
                return Result.ok();
            } else {
               
                return error(CONFLICT);
            }
        } else {
            bc.upload(bdata);
            cacheBlobData(blobId, bdata);
         
            return Result.ok();
        }
    } catch (Exception e) {
      
        return error(INTERNAL_ERROR);
    }
}

@Override
public Result<byte[]> download(String blobId, String token) {
   

    if (!isBlobIdValid(blobId, token)) {
        return error(FORBIDDEN);
    }

    try {
        // Attempt to retrieve cached data
        BinaryData cachedData = getCachedBytes(blobId);
        if (cachedData != null) {
            return ok(cachedData.toBytes());
        }

        BlobClient bc = containerClient.getBlobClient(blobId);

        // Download blob data if it exists
        if (bc.exists()) {
            byte[] content = bc.downloadContent().toBytes();
        
            return Result.ok(content);
        } else {
          
            return error(NOT_FOUND);
        }
    } catch (Exception e) {
        Log.severe("Download error: " + e.getMessage());
        return error(INTERNAL_ERROR);
    }
}

@Override
public Result<Void> delete(String blobId, String token) {
    Log.info(() -> format("Deleting blob: blobId = %s, token = %s", blobId, token));

    if (!isBlobIdValid(blobId, token)) {
        return error(FORBIDDEN);
    }

    try {
        BlobClient bc = containerClient.getBlobClient(blobId);

        // Check if the blob exists before attempting deletion
        if (bc.exists()) {
            bc.delete();
			clearCachedBlob(blobId);
          
            return Result.ok();
        } else {
          
            return error(NOT_FOUND);
        }
    } catch (Exception e) {
        Log.severe("Error during deletion: " + e.getMessage());
        return error(INTERNAL_ERROR);
    }
}



@Override
public Result<Void> deleteAllBlobs(String userId, String token) {
    Log.info(() -> format("Initiating bulk deletion for all blobs of user: userId = %s, token = %s", userId, token));

    if (!Token.isValid(token, userId)) {
        return error(FORBIDDEN);
    }

    List<String> blobIds = JavaShorts.getInstance().getShorts(userId).value();

    for (String blobId : blobIds) {
        try {
            BlobClient bc = containerClient.getBlobClient(blobId);

            if (bc.exists()) {
                bc.delete();
                clearCachedBlob(blobId);
                Log.info(() -> format("Deleted blob: %s", blobId));
            }
        } catch (Exception e) {
        
            return error(INTERNAL_ERROR);
        }
    }

    return Result.ok();
}

private boolean isBlobIdValid(String blobId, String token) {		
    return Token.isValid(token, blobId);
}

	
private void cacheBlobData(String blobId, BinaryData data) {
    try (Jedis jed = RedisCache.getCachePool().getResource()) {
        jed.set("bytes:" + blobId, JSON.encode(data));
        
    } catch (JedisException e) {
    
    }
}


	



	private BinaryData getCachedBytes(String blobId) {
		try (Jedis jed = RedisCache.getCachePool().getResource()) {
			String cachedDataJson = jed.get("bytes:" + blobId);
			// Verifica se há dados em cache
			if (cachedDataJson != null) {
				Log.info(() -> format("Cache hit for blobId: %s", blobId));
				return JSON.decode(cachedDataJson, BinaryData.class);
			} else {
				Log.info(() -> format("Cache miss for blobId: %s", blobId));
			}
		} catch (JedisException e) {
			Log.warning("Redis access failed, unable to retrieve cached blob for ID " + blobId + ": " + e.getMessage());
		}
		return null;
	}

	private void clearCachedBlob(String blobId) {
		try (Jedis jed = RedisCache.getCachePool().getResource()) {
			jed.del("bytes:" + blobId);
			Log.info(() -> format("Cleared cached data for blobId: %s", blobId));
		} catch (JedisException e) {
			Log.warning("Failed to clear cached data in Redis for blobId " + blobId + ": " + e.getMessage());
		}
	}}
	
