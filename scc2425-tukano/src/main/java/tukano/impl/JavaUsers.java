package tukano.impl;

import static java.lang.String.format;
import static tukano.api.Result.error;
import static tukano.api.Result.ErrorCode.*;
import redis.clients.jedis.Jedis;
import tukano.api.Result;
import tukano.api.Shorts;
import tukano.api.User;
import tukano.api.Users;
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
import java.util.logging.Logger;
import com.fasterxml.jackson.core.type.TypeReference;

public class JavaUsers implements Users {

    private static final Logger Log = Logger.getLogger(JavaUsers.class.getName());
    private static JavaUsers instance;
    private final Connection connection;
    private static final String USER_CACHE_PREFIX = "user:";
    private final Shorts shorts;

    // Construtor singleton
    synchronized public static JavaUsers getInstance() {
        if (instance == null) {
            instance = new JavaUsers();
        }
        return instance;
    }

    private JavaUsers() {
        try {
            shorts = JavaShorts.getInstance();
            connection = DB_PostgresSQL.getConnection();  
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Result<String> createUser(User user) {
        Log.info(() -> format("createUser : %s\n", user));

        if (badUserInfo(user)) {
            return error(BAD_REQUEST);
        }

    // Query de inserção
    String insertCmd = "INSERT INTO users (user_id, pwd, email, display_name) VALUES (?, ?, ?, ?)";

    try {
        // Inserir usuário no banco de dados
        if (executeInsertUser(user, insertCmd)) {
            // Armazenar no cache após inserção bem-sucedida
            cacheUser(user);
            Log.info("User created and cached successfully with ID: " + user.getid());
            return Result.ok(user.getid());
        } else {
            // Caso não seja possível inserir o usuário (nenhuma linha afetada)
            Log.warning("Failed to create user: No rows affected.");
            return Result.error(Result.ErrorCode.INTERNAL_ERROR);
        }
    } catch (SQLException e) {
        Log.severe("Database error during user creation: " + e.getMessage());
        return Result.error(Result.ErrorCode.INTERNAL_ERROR);
    }
}


	// Método auxiliar para realizar a inserção do usuário no banco
	private boolean executeInsertUser(User user, String query) throws SQLException {
		try (PreparedStatement ps = connection.prepareStatement(query)) {
			ps.setString(1, user.getid());
			ps.setString(2, user.getPwd());
			ps.setString(3, user.getEmail());
			ps.setString(4, user.getDisplayName());
			return ps.executeUpdate() > 0; // Retorna verdadeiro se a inserção afetar ao menos uma linha
		}
	}
	@Override
	public Result<User> getUser(String userId, String pwd) {
		Log.info(() -> format("Retrieving user with ID: %s\n", userId));
	
		// Primeiro, tenta obter o usuário do cache
		User userFromCache = fetchUserFromCache(userId, pwd);
		if (userFromCache != null) {
			Log.info("User retrieved from cache: " + userId);
			return Result.ok(userFromCache);
		}
	
		// Query para buscar o usuário no banco de dados
		String sqlQuery = "SELECT user_id, pwd, email, display_name FROM users WHERE user_id = ? AND pwd = ?";
		try {
			User userFromDB = fetchUserFromDatabase(userId, pwd, sqlQuery);
			if (userFromDB != null) {
				// Cacheia o usuário após a recuperação do banco de dados
				cacheUser(userFromDB);
				Log.info("User retrieved from database and cached: " + userId);
				return Result.ok(userFromDB);
			} else {
				Log.info("User not found or password incorrect for ID: " + userId);
				return Result.error(Result.ErrorCode.NOT_FOUND);
			}
		} catch (SQLException e) {
			Log.severe("Error fetching user from database: " + e.getMessage());
			return Result.error(Result.ErrorCode.INTERNAL_ERROR);
		}
	}
	
	// Método auxiliar para verificar o cache
	private User fetchUserFromCache(String userId, String pwd) {
		User cachedUser = getCachedUser(userId);
		return (cachedUser != null && pwd.equals(cachedUser.getPwd())) ? cachedUser : null;
	}
	
	// Método auxiliar para buscar o usuário no banco de dados
	private User fetchUserFromDatabase(String userId, String pwd, String query) throws SQLException {
		try (PreparedStatement ps = connection.prepareStatement(query)) {
			ps.setString(1, userId);
			ps.setString(2, pwd);
			try (ResultSet resultSet = ps.executeQuery()) {
				if (resultSet.next()) {
					return new User(
							resultSet.getString("user_id"),
							resultSet.getString("pwd"),
							resultSet.getString("email"),
							resultSet.getString("display_name")
					);
				}
			}
		}
		return null; // Retorna null se o usuário não foi encontrado
	}
	
	@Override
	public Result<User> updateUser(String userId, String pwd, User other) {
		Log.info(() -> format("Attempting to update user with ID: %s\n", userId));
	
		String sql = "UPDATE users SET pwd = ?, email = ?, display_name = ? WHERE user_id = ? AND pwd = ?";
		try {
			int rowsAffected = executeUserUpdate(sql, other, userId, pwd);
			if (rowsAffected > 0) {
				cacheUser(other);
				Log.info("User updated successfully: " + userId);
				return Result.ok(other);
			} else {
				Log.info("Update failed: User not found or incorrect password for ID: " + userId);
				return Result.error(Result.ErrorCode.NOT_FOUND);
			}
		} catch (SQLException e) {
			Log.severe("Error updating user: " + e.getMessage());
			return Result.error(Result.ErrorCode.INTERNAL_ERROR);
		}
	}
	
	// Método auxiliar para executar a atualização do usuário
	private int executeUserUpdate(String query, User user, String userId, String password) throws SQLException {
		try (PreparedStatement ps = connection.prepareStatement(query)) {
			ps.setString(1, user.getPwd());
			ps.setString(2, user.getEmail());
			ps.setString(3, user.getDisplayName());
			ps.setString(4, userId);
			ps.setString(5, password);
			return ps.executeUpdate();  // Retorna o número de linhas afetadas
		}
	}
	

    @Override
public Result<User> deleteUser(String userId, String pwd) {
    Log.info(() -> format("Deleting user with ID: %s\n", userId));

    String delCmd = "DELETE FROM users WHERE user_id = ? AND pwd = ?";
    try {
        if (performUserDeletion(delCmd, userId, pwd)) {
            removeCachedUser(userId);
            shorts.deleteAllShorts(userId, pwd, RestShorts.TOKEN);
            Log.info("User successfully deleted: " + userId);
            return Result.ok();
        } else {
            Log.warning("Delete failed: User not found or incorrect password for ID: " + userId);
            return Result.error(Result.ErrorCode.NOT_FOUND);
        }
    } catch (SQLException e) {
        Log.severe("Error deleting user: " + e.getMessage());
        return Result.error(Result.ErrorCode.INTERNAL_ERROR);
    }
}

// Método auxiliar para executar a exclusão do usuário
private boolean performUserDeletion(String query, String userId, String pwd) throws SQLException {
    try (PreparedStatement stmt = connection.prepareStatement(query)) {
        stmt.setString(1, userId);
        stmt.setString(2, pwd);
        return stmt.executeUpdate() > 0;  // Retorna verdadeiro se uma linha foi deletada
    }
}

	@Override
	public Result<List<User>> searchUsers(String pattern) {
		Log.info(() -> format("Searching for users with pattern: %s\n", pattern));
	
		// Tenta buscar os resultados do cache primeiro
		List<User> cachedResults = fetchCachedSearchResults(pattern);
		if (cachedResults != null) {
			Log.info("Search results retrieved from cache for pattern: " + pattern);
			return Result.ok(cachedResults);
		}
	
		// Query para busca no banco de dados
		String searchQuery = "SELECT user_id, pwd, email, display_name FROM users WHERE display_name ILIKE ? OR email ILIKE ?";
		try {
			List<User> usersFromDB = performUserSearch(pattern, searchQuery);
			cacheSearchResults(pattern, usersFromDB);
			Log.info("Search completed in database for pattern: " + pattern);
			return Result.ok(usersFromDB);
		} catch (SQLException e) {
			Log.severe("Database error during search: " + e.getMessage());
			return Result.error(Result.ErrorCode.INTERNAL_ERROR);
		}
	}
	
	// Método auxiliar para buscar os resultados no cache
	private List<User> fetchCachedSearchResults(String pattern) {
		String cacheKey = "user_search_" + pattern.toUpperCase();
		try (Jedis jed = RedisCache.getCachePool().getResource()) {
			String cachedJson = jed.get(cacheKey);
			return cachedJson != null ? JSON.decode(cachedJson, new TypeReference<List<User>>() {}) : null;
		} catch (Exception e) {
			Log.warning("Failed to access Redis cache, continuing with database search.");
			return null;
		}
	}
	
	// Método auxiliar para realizar a busca no banco de dados
	private List<User> performUserSearch(String pattern, String query) throws SQLException {
		List<User> users = new ArrayList<>();
		try (PreparedStatement stmt = connection.prepareStatement(query)) {
			stmt.setString(1, "%" + pattern + "%");
			stmt.setString(2, "%" + pattern + "%");
			try (ResultSet rs = stmt.executeQuery()) {
				while (rs.next()) {
					users.add(new User(
						rs.getString("user_id"),
						rs.getString("pwd"),
						rs.getString("email"),
						rs.getString("display_name")
					));
				}
			}
		}
		return users;
	}
	
	// Método auxiliar para cachear os resultados da busca
	private void cacheSearchResults(String pattern, List<User> users) {
		String cacheKey = "user_search_" + pattern.toUpperCase();
		try (Jedis jed = RedisCache.getCachePool().getResource()) {
			jed.setex(cacheKey, 3600, JSON.encode(users));
		} catch (Exception e) {
			Log.warning("Failed to cache search results in Redis.");
		}
	}
	

    private boolean badUserInfo(User user) {
        return (user.id() == null || user.pwd() == null || user.displayName() == null || user.email() == null);
    }

    private void cacheUser(User user) {
        try (Jedis jed = RedisCache.getCachePool().getResource()) {
            jed.set(USER_CACHE_PREFIX + user.getid(), JSON.encode(user));
        }
    }

    private User getCachedUser(String userId) {
        try (Jedis jed = RedisCache.getCachePool().getResource()) {
            String userJson = jed.get(USER_CACHE_PREFIX + userId);
            return userJson != null ? JSON.decode(userJson, User.class) : null;
        }
    }

    private void removeCachedUser(String userId) {
        try (Jedis jed = RedisCache.getCachePool().getResource()) {
            jed.del(USER_CACHE_PREFIX + userId);
        }
    }
}
