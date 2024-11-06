package tukano.impl;

import static java.lang.String.format;
import static tukano.api.Result.error;
import static tukano.api.Result.ok;
import static tukano.api.Result.ErrorCode.*;

import com.fasterxml.jackson.core.type.TypeReference;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisException;
import tukano.api.Blobs;
import tukano.api.Result;
import tukano.api.Short;
import tukano.api.Shorts;
import tukano.api.User;
import tukano.api.rest.RestShorts;
import dataBaseConection.DB_PostgresSQL;
import cache.RedisCache;
import utils.JSON;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.logging.Logger;

public class JavaShorts implements Shorts {

    private static final Logger Log = Logger.getLogger(JavaShorts.class.getName());
    private static JavaShorts instance;
    private final Connection connection;
    private static final String SHORT_CACHE_PREFIX = "short:";
    private static final String FOLLOWERS_CACHE_PREFIX = "followers_user:";

    private JavaShorts() {
        try {
            this.connection = DB_PostgresSQL.getConnection();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public static synchronized Shorts getInstance() {
        if (instance == null) {
            instance = new JavaShorts();
        }
        return instance;
    }

	private Result<User> validateUserForAction(String userId, String password) {
		// Usa o método okUser existente para validar o usuário
		Result<User> userResult = okUser(userId, password);
		if (!userResult.isOK()) {
			Log.warning(() -> format("User validation failed for ID: %s with provided password.", userId));
		}
		return userResult;
	}

	
	@Override
	public Result<Short> createShort(String userId, String password) {
		Log.info(() -> format("Initiating short creation for user: %s\n", userId));
	
		Result<User> userResult = validateUserForAction(userId, password);
		if (!userResult.isOK()) {
			return Result.error(userResult.error());
		}
	
		Short newShort = generateNewShort(userId);
		try {
			if (storeShortInDatabase(newShort)) {
				cacheShort(newShort);
				return ok(newShort);
			} else {
				return Result.error(Result.ErrorCode.INTERNAL_ERROR);
			}
		} catch (SQLException e) {
			Log.severe("Failed to create short due to database error: " + e.getMessage());
			return Result.error(Result.ErrorCode.INTERNAL_ERROR);
		}
	}
	
	// Gera um novo Short com um ID e URL únicos
	private Short generateNewShort(String userId) {
		String uniqueId = format("%s+%s", userId, UUID.randomUUID());
		String blobUrl = format("%s/%s", Blobs.LINK, uniqueId);
		return new Short(uniqueId, userId, blobUrl);
	}
	
	// Insere o Short no banco de dados
	private boolean storeShortInDatabase(Short shrt) throws SQLException {
		String sql = "INSERT INTO shorts (short_id, user_id, blob_url) VALUES (?, ?, ?)";
		try (PreparedStatement stmt = connection.prepareStatement(sql)) {
			stmt.setString(1, shrt.getid());
			stmt.setString(2, shrt.getOwnerId());
			stmt.setString(3, shrt.getBlobUrl());
			return stmt.executeUpdate() > 0;
		}
	}
	
    @Override
    public Result<Short> getShort(String shortId) {
        Log.info(() -> format("getShort : shortId = %s\n", shortId));

        if (shortId == null) {
            return error(BAD_REQUEST);
        }

        Short cachedShort = getCachedShort(shortId);
        if (cachedShort != null) {
            return ok(cachedShort);
        }

		try {
			Short shortFromDB = fetchShortFromDatabase(shortId);
			if (shortFromDB != null) {
				cacheShort(shortFromDB);
				return ok(shortFromDB);
			} else {
				return Result.error(NOT_FOUND);
			}
		} catch (SQLException e) {
			Log.severe("Error retrieving short from database: " + e.getMessage());
			return Result.error(Result.ErrorCode.INTERNAL_ERROR);
		}
	}
	
	// Método auxiliar para buscar o Short do banco de dados
	private Short fetchShortFromDatabase(String shortId) throws SQLException {
		String sql = "SELECT short_id, user_id, blob_url FROM shorts WHERE short_id = ?";
		try (PreparedStatement stmt = connection.prepareStatement(sql)) {
			stmt.setString(1, shortId);
			try (ResultSet rs = stmt.executeQuery()) {
				if (rs.next()) {
					return new Short(
						rs.getString("short_id"),
						rs.getString("user_id"),
						rs.getString("blob_url")
					);
				}
			}
		}
		return null;
	}
    @Override
    public Result<Void> deleteShort(String shortId, String password) {
        Log.info(() -> format("deleteShort : shortId = %s, pwd = %s\n", shortId, password));

        Result<Short> shrt = getShort(shortId);
        if (shrt.error().equals(NOT_FOUND)) {
            return Result.error(NOT_FOUND);
        }

        Result<User> user = okUser(shrt.value().getOwnerId(), password);
        if (!user.isOK()) {
            return Result.error(FORBIDDEN);
        }

		try {
			performShortDeletion(shortId);
			removeCachedShort(shortId);
			JavaBlobs.getInstance().delete(shortId, RestShorts.TOKEN);
			return ok();
		} catch (SQLException e) {
			Log.severe("Failed to delete short due to database error: " + e.getMessage());
			return Result.error(Result.ErrorCode.INTERNAL_ERROR);
		}
	}
	
	// Método auxiliar para deletar o Short e likes associados
	private void performShortDeletion(String shortId) throws SQLException {
		String deleteLikesSQL = "DELETE FROM likes WHERE short_id = ?";
		String deleteShortSQL = "DELETE FROM shorts WHERE short_id = ?";
	
		try (PreparedStatement stmtLikes = connection.prepareStatement(deleteLikesSQL);
			 PreparedStatement stmtShort = connection.prepareStatement(deleteShortSQL)) {
			stmtLikes.setString(1, shortId);
			stmtLikes.executeUpdate();
	
			stmtShort.setString(1, shortId);
			stmtShort.executeUpdate();
		}
	}

	@Override
	public Result<List<String>> getShorts(String userId) {
		Log.info(() -> format("Retrieving shorts for user: %s", userId));
		
		// Verifica se o usuário é válido usando o Result<Void>
		Result<Void> userValidation = okUser(userId);
		if (!userValidation.isOK()) {
			return Result.error(NOT_FOUND);
		}
	
		// Tenta buscar a lista de IDs dos shorts no cache
		String cacheKey = "shorts_user:" + userId;
		List<String> cachedShortIds = getCachedListFromCache(cacheKey);
		if (cachedShortIds != null) {
			return Result.ok(cachedShortIds);
		}
	
		// Consulta o banco de dados para obter os IDs dos shorts
		String querySQL = "SELECT short_id FROM shorts WHERE user_id = ?";
		List<String> shortIds = new ArrayList<>();
		try (PreparedStatement pstmt = connection.prepareStatement(querySQL)) {
			pstmt.setString(1, userId);
			try (ResultSet rs = pstmt.executeQuery()) {
				while (rs.next()) {
					shortIds.add(rs.getString("short_id"));
				}
			}
			// Armazena a lista no cache
			cacheListInCache(cacheKey, shortIds);
			return Result.ok(shortIds);
		} catch (SQLException e) {
			Log.severe("Error retrieving shorts from database: " + e.getMessage());
			return Result.error(Result.ErrorCode.INTERNAL_ERROR);
		}
	}
	
	
	@Override
	public Result<Void> follow(String userId1, String userId2, boolean isFollowing, String password) {
		Log.info(() -> format("User %s attempting to follow/unfollow user %s", userId1, userId2));
	
		// Valida o usuário que está seguindo (userId1) e autentica a senha
		Result<User> followerValidation = okUser(userId1, password);
		if (!followerValidation.isOK()) {
			return Result.error(followerValidation.error());
		}
	
		// Valida o usuário que será seguido (userId2)
		Result<Void> followeeValidation = okUser(userId2);
		if (!followeeValidation.isOK()) {
			return Result.error(NOT_FOUND);
		}
	
		// Define a consulta SQL para seguir ou deixar de seguir
		String sql = isFollowing ? 
			"INSERT INTO following (follower, followee) VALUES (?, ?)" : 
			"DELETE FROM following WHERE follower = ? AND followee = ?";
	
		// Executa a ação de seguir/deixar de seguir
		try (PreparedStatement pstmt = connection.prepareStatement(sql)) {
			pstmt.setString(1, userId1);
			pstmt.setString(2, userId2);
			pstmt.executeUpdate();
	
			// Limpa o cache para atualizar a lista de seguidores
			unfollowCacheForUser(userId1);
			return Result.ok();
		} catch (SQLException e) {
			Log.severe("Error during follow/unfollow action: " + e.getMessage());
			return Result.error(Result.ErrorCode.INTERNAL_ERROR);
		}
	}
	


@Override
public Result<List<String>> followers(String userId, String password) {
    Log.info(() -> format("Retrieving followers for user: %s", userId));

    // Autentica o usuário para garantir que ele tem acesso
    Result<User> userValidation = okUser(userId, password);
    if (!userValidation.isOK()) {
        return Result.error(userValidation.error());
    }

    // Tenta buscar a lista de seguidores no cache
    String cacheKey = "followers_user:" + userId;
    List<String> cachedFollowers = getCachedListFromCache(cacheKey);
    if (cachedFollowers != null) {
        return Result.ok(cachedFollowers);
    }

    // Consulta o banco de dados para obter a lista de seguidores
    String querySQL = "SELECT follower FROM following WHERE followee = ?";
    List<String> followers = new ArrayList<>();
    try (PreparedStatement pstmt = connection.prepareStatement(querySQL)) {
        pstmt.setString(1, userId);
        try (ResultSet rs = pstmt.executeQuery()) {
            while (rs.next()) {
                followers.add(rs.getString("follower"));
            }
        }
        // Armazena a lista de seguidores no cache
        cacheListInCache(cacheKey, followers);
        return Result.ok(followers);
    } catch (SQLException e) {
        Log.severe("Error retrieving followers from database: " + e.getMessage());
        return Result.error(Result.ErrorCode.INTERNAL_ERROR);
    }
}


@Override
public Result<Void> like(String shortId, String userId, boolean isLiked, String password) {
    Log.info(() -> format("User %s attempting to like/unlike short: %s", userId, shortId));

    // Autentica o usuário para garantir que ele pode curtir/descurtir
    Result<User> userValidation = okUser(userId, password);
    if (!userValidation.isOK()) {
        return Result.error(userValidation.error());
    }

    // Verifica se o Short existe
    Result<Short> shortValidation = getShort(shortId);
    if (!shortValidation.isOK()) {
        return Result.error(NOT_FOUND);
    }

    // Define a consulta SQL para curtir ou remover a curtida
    String sql = isLiked ? 
        "INSERT INTO likes (user_id, short_id, owner_id) VALUES (?, ?, ?)" : 
        "DELETE FROM likes WHERE user_id = ? AND short_id = ?";

    // Executa a ação de curtir/descurtir
    try (PreparedStatement pstmt = connection.prepareStatement(sql)) {
        pstmt.setString(1, userId);
        pstmt.setString(2, shortId);
        if (isLiked) {
            pstmt.setString(3, shortValidation.value().getOwnerId());
        }
        pstmt.executeUpdate();

        // Limpa o cache das curtidas para o short específico
        try (Jedis jedis = RedisCache.getCachePool().getResource()) {
            jedis.del("likes_short:" + shortId);
        } catch (JedisException e) {
            Log.warning("Failed to update Redis cache.");
        }
        return Result.ok();
    } catch (SQLException e) {
        Log.severe("Error performing like/unlike action: " + e.getMessage());
        return Result.error(Result.ErrorCode.INTERNAL_ERROR);
    }
}

@Override
public Result<List<String>> likes(String shortId, String password) {
    Log.info(() -> format("Retrieving likes for short: %s", shortId));

    // Verifica se o Short existe
    Result<Short> shortValidation = getShort(shortId);
    if (!shortValidation.isOK()) {
        return Result.error(NOT_FOUND);
    }

    // Autentica o dono do Short para visualizar as curtidas
    Result<User> ownerValidation = okUser(shortValidation.value().getOwnerId(), password);
    if (ownerValidation.error() == FORBIDDEN) {
        return Result.error(FORBIDDEN);
    } else if (!ownerValidation.isOK()) {
        return Result.error(BAD_REQUEST);
    }

    // Tenta buscar a lista de curtidas no cache
    String cacheKey = "likes_short:" + shortId;
    List<String> cachedLikes = getCachedListFromCache(cacheKey);
    if (cachedLikes != null) {
        return Result.ok(cachedLikes);
    }

    // Consulta o banco de dados para obter a lista de curtidas
    String querySQL = "SELECT user_id FROM likes WHERE short_id = ?";
    List<String> likesList = new ArrayList<>();
    try (PreparedStatement pstmt = connection.prepareStatement(querySQL)) {
        pstmt.setString(1, shortId);
        try (ResultSet rs = pstmt.executeQuery()) {
            while (rs.next()) {
                likesList.add(rs.getString("user_id"));
            }
        }
        // Armazena a lista de curtidas no cache
        cacheListInCache(cacheKey, likesList);
        return Result.ok(likesList);
    } catch (SQLException e) {
        Log.severe("Error retrieving likes from database: " + e.getMessage());
        return Result.error(Result.ErrorCode.INTERNAL_ERROR);
    }
}


@Override
public Result<List<String>> getFeed(String userId, String password) {
    Log.info(() -> format("Retrieving feed for user: %s", userId));

    // Autentica o usuário para garantir que ele tenha acesso ao feed
    Result<User> userValidation = okUser(userId, password);
    if (!userValidation.isOK()) {
        return Result.error(userValidation.error());
    }

    // Tenta recuperar o feed do cache
    String cacheKey = "feed_user:" + userId;
    List<String> cachedFeed = getCachedListFromCache(cacheKey);
    if (cachedFeed != null) {
        return Result.ok(cachedFeed);
    }

    // Consulta o banco de dados para obter o feed
    String queryFeedSQL = """
        SELECT short_id FROM shorts
        WHERE user_id IN (SELECT followee FROM following WHERE follower = ?)
        ORDER BY created_at DESC
    """;
    List<String> feedList = new ArrayList<>();
    try (PreparedStatement pstmt = connection.prepareStatement(queryFeedSQL)) {
        pstmt.setString(1, userId);
        try (ResultSet rs = pstmt.executeQuery()) {
            while (rs.next()) {
                feedList.add(rs.getString("short_id"));
            }
        }
        // Armazena o feed no cache para futuras consultas
        cacheListInCache(cacheKey, feedList);
        return Result.ok(feedList);
    } catch (SQLException e) {
        Log.severe("Error retrieving feed from database: " + e.getMessage());
        return Result.error(Result.ErrorCode.INTERNAL_ERROR);
    }
}

   
private Result<Void> deleteAllShorts(String userId) {
    Log.info(() -> format("Deleting all shorts and related data for user: %s", userId));

    // Consultas SQL para deletar todos os dados relacionados aos shorts do usuário
    String deleteLikesSQL = "DELETE FROM likes WHERE short_id IN (SELECT short_id FROM shorts WHERE user_id = ?)";
    String deleteShortsSQL = "DELETE FROM shorts WHERE user_id = ?";
    String deleteFollowingAsFollowerSQL = "DELETE FROM following WHERE follower = ?";
    String deleteFollowingAsFolloweeSQL = "DELETE FROM following WHERE followee = ?";

    try (
        PreparedStatement pstmtLikes = connection.prepareStatement(deleteLikesSQL);
        PreparedStatement pstmtShorts = connection.prepareStatement(deleteShortsSQL);
        PreparedStatement pstmtFollowingAsFollower = connection.prepareStatement(deleteFollowingAsFollowerSQL);
        PreparedStatement pstmtFollowingAsFollowee = connection.prepareStatement(deleteFollowingAsFolloweeSQL)
    ) {
        // Deleta os likes associados aos shorts do usuário
        pstmtLikes.setString(1, userId);
        pstmtLikes.executeUpdate();

        // Deleta os shorts do usuário
        pstmtShorts.setString(1, userId);
        pstmtShorts.executeUpdate();

        // Deleta o usuário como seguidor
        pstmtFollowingAsFollower.setString(1, userId);
        pstmtFollowingAsFollower.executeUpdate();

        // Deleta o usuário como seguido
        pstmtFollowingAsFollowee.setString(1, userId);
        pstmtFollowingAsFollowee.executeUpdate();

        // Invalida o cache do usuário
        invalidateCacheForUser(userId);
        return Result.ok();
    } catch (SQLException e) {
        Log.severe("Error deleting all shorts and related data: " + e.getMessage());
        return Result.error(Result.ErrorCode.INTERNAL_ERROR);
    }
}


	@Override
	public Result<Void> deleteAllShorts(String userId, String password, String token) {
		Log.info(() -> format("deleteAllShorts : userId = %s, password = %s, token = %s\n", userId, password, token));
		return deleteAllShorts(userId);
	}

    // Auxiliary Methods
	private void cacheShort(Short shrt) {
		String cacheKey = SHORT_CACHE_PREFIX + shrt.getid();
		try (Jedis jedis = RedisCache.getCachePool().getResource()) {
			jedis.set(cacheKey, JSON.encode(shrt));
			Log.info("Successfully cached short with ID: " + shrt.getid());
		} catch (JedisException e) {
			Log.warning("Error caching short in Redis for ID: " + shrt.getid() + " - " + e.getMessage());
		}
	}
	

	private Short getCachedShort(String shortId) {
		String cacheKey = SHORT_CACHE_PREFIX + shortId;
		try (Jedis jedis = RedisCache.getCachePool().getResource()) {
			String shortJson = jedis.get(cacheKey);
			if (shortJson != null) {
				Log.info("Cache hit for short ID: " + shortId);
				return JSON.decode(shortJson, Short.class);
			}
		} catch (JedisException e) {
			Log.warning("Redis error retrieving cached short for ID: " + shortId + " - " + e.getMessage());
		}
		Log.info("Cache miss for short ID: " + shortId);
		return null;
	}
	
	private void removeCachedShort(String shortId) {
		String cacheKey = SHORT_CACHE_PREFIX + shortId;
		try (Jedis jedis = RedisCache.getCachePool().getResource()) {
			jedis.del(cacheKey);
			Log.info("Removed cached short for ID: " + shortId);
		} catch (JedisException e) {
			Log.warning("Failed to remove cached short in Redis for ID: " + shortId + " - " + e.getMessage());
		}
	}
	
	private List<String> getCachedListFromCache(String cacheKey) {
		try (Jedis jedis = RedisCache.getCachePool().getResource()) {
			String cachedJson = jedis.get(cacheKey);
			if (cachedJson != null) {
				Log.info("Cache hit for key: " + cacheKey);
				return JSON.decode(cachedJson, new TypeReference<List<String>>() {});
			}
		} catch (JedisException e) {
			Log.warning("Redis access error for key: " + cacheKey + " - " + e.getMessage());
		}
		Log.info("Cache miss for key: " + cacheKey);
		return null;
	}
	

	private void cacheListInCache(String cacheKey, List<String> list) {
		try (Jedis jedis = RedisCache.getCachePool().getResource()) {
			jedis.setex(cacheKey, 3600, JSON.encode(list));
			Log.info("Successfully cached list under key: " + cacheKey);
		} catch (JedisException e) {
			Log.warning("Error caching list in Redis for key: " + cacheKey + " - " + e.getMessage());
		}
	}
	

	private void unfollowCacheForUser(String userId) {
		String feedCacheKey = "feed_user:" + userId;
		String followersCacheKey = FOLLOWERS_CACHE_PREFIX + userId;
		try (Jedis jedis = RedisCache.getCachePool().getResource()) {
			jedis.del(feedCacheKey);
			jedis.del(followersCacheKey);
			Log.info("Cleared feed and followers cache for user: " + userId);
		} catch (JedisException e) {
			Log.warning("Failed to clear cache for user " + userId + " - " + e.getMessage());
		}
	}
	

	private void invalidateCacheForUser(String userId) {
		try (Jedis jedis = RedisCache.getCachePool().getResource()) {
			// Chaves de cache específicas do usuário
			jedis.del("shorts_user:" + userId);
			jedis.del("followers_user:" + userId);
			jedis.del("feed_user:" + userId);
	
			// Remove o cache de likes para cada short do usuário
			String queryUserShortsSQL = "SELECT short_id FROM shorts WHERE user_id = ?";
			try (PreparedStatement pstmt = connection.prepareStatement(queryUserShortsSQL)) {
				pstmt.setString(1, userId);
				try (ResultSet rs = pstmt.executeQuery()) {
					while (rs.next()) {
						jedis.del("likes_short:" + rs.getString("short_id"));
					}
				}
			}
			Log.info("Successfully invalidated all caches for user: " + userId);
		} catch (JedisException e) {
			Log.warning("Redis error invalidating cache for user " + userId + " - " + e.getMessage());
		} catch (SQLException e) {
			Log.warning("Database error invalidating cache for user " + userId + " - " + e.getMessage());
		}
	}
	

    protected Result<User> okUser(String userId, String pwd) {
        return JavaUsers.getInstance().getUser(userId, pwd);
    }

	

    private Result<Void> okUser(String userId) {
        var res = okUser(userId, "");
        return res.isOK() || res.error() == FORBIDDEN ? ok() : error(res.error());
    }

	
}
