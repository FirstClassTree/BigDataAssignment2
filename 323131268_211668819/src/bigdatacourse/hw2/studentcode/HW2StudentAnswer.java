package bigdatacourse.hw2.studentcode;

import java.io.BufferedReader;
import java.io.FileReader;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.json.JSONArray;
import org.json.JSONObject;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;

import bigdatacourse.hw2.HW2API;

public class HW2StudentAnswer implements HW2API {

	// general consts
	public static final String NOT_AVAILABLE_VALUE = "na";

	// CQL table names
	private static final String TABLE_ITEMS = "items";
	private static final String TABLE_REVIEWS_BY_USER = "reviews_by_user";
	private static final String TABLE_REVIEWS_BY_ITEM = "reviews_by_item";

	// CQL create table statements
	private static final String CQL_CREATE_ITEMS = 
		"CREATE TABLE IF NOT EXISTS " + TABLE_ITEMS + " (" +
		"asin TEXT PRIMARY KEY, " +
		"title TEXT, " +
		"image_url TEXT, " +
		"categories SET<TEXT>, " +
		"description TEXT" +
		")";

		private static final String CQL_CREATE_REVIEWS_BY_USER = 
		"CREATE TABLE IF NOT EXISTS " + TABLE_REVIEWS_BY_USER + " (" +
		"reviewer_id TEXT, " +
		"review_time TIMESTAMP, " +
		"asin TEXT, " +
		"reviewer_name TEXT, " +
		"rating DOUBLE, " +
		"summary TEXT, " +
		"review_text TEXT, " +
		"PRIMARY KEY (reviewer_id, review_time, asin)" +
		") WITH CLUSTERING ORDER BY (review_time DESC, asin ASC)";

		private static final String CQL_CREATE_REVIEWS_BY_ITEM = 
		"CREATE TABLE IF NOT EXISTS " + TABLE_REVIEWS_BY_ITEM + " (" +
		"asin TEXT, " +
		"review_time TIMESTAMP, " +
		"reviewer_id TEXT, " +
		"reviewer_name TEXT, " +
		"rating DOUBLE, " +
		"summary TEXT, " +
		"review_text TEXT, " +
		"PRIMARY KEY (asin, review_time, reviewer_id)" +
		") WITH CLUSTERING ORDER BY (review_time DESC, reviewer_id ASC)";

	// CQL prepared statement strings
	private static final String CQL_INSERT_ITEM = 
		"INSERT INTO " + TABLE_ITEMS + " (asin, title, image_url, categories, description) VALUES (?, ?, ?, ?, ?)";

	private static final String CQL_INSERT_REVIEW_BY_USER = 
		"INSERT INTO " + TABLE_REVIEWS_BY_USER + " (reviewer_id, review_time, asin, reviewer_name, rating, summary, review_text) VALUES (?, ?, ?, ?, ?, ?, ?)";

	private static final String CQL_INSERT_REVIEW_BY_ITEM = 
		"INSERT INTO " + TABLE_REVIEWS_BY_ITEM + " (asin, review_time, reviewer_id, reviewer_name, rating, summary, review_text) VALUES (?, ?, ?, ?, ?, ?, ?)";

	private static final String CQL_SELECT_ITEM = 
		"SELECT * FROM " + TABLE_ITEMS + " WHERE asin = ?";

	private static final String CQL_SELECT_REVIEWS_BY_USER = 
		"SELECT * FROM " + TABLE_REVIEWS_BY_USER + " WHERE reviewer_id = ?";

	private static final String CQL_SELECT_REVIEWS_BY_ITEM = 
		"SELECT * FROM " + TABLE_REVIEWS_BY_ITEM + " WHERE asin = ?";

	// cassandra session
	private CqlSession session;

	// prepared statements
	private PreparedStatement pstmtInsertItem;
	private PreparedStatement pstmtInsertReviewByUser;
	private PreparedStatement pstmtInsertReviewByItem;
	private PreparedStatement pstmtSelectItem;
	private PreparedStatement pstmtSelectReviewsByUser;
	private PreparedStatement pstmtSelectReviewsByItem;

	@Override
	public void connect(String pathAstraDBBundleFile, String username, String password, String keyspace) {
		if (session != null) {
			System.out.println("ERROR - cassandra is already connected");
			return;
		}

		System.out.println("Initializing connection to Cassandra...");

		this.session = CqlSession.builder()
				.withCloudSecureConnectBundle(Paths.get(pathAstraDBBundleFile))
				.withAuthCredentials(username, password)
				.withKeyspace(keyspace)
				.build();

		System.out.println("Initializing connection to Cassandra... Done");
	}

	@Override
	public void close() {
		if (session == null) {
			System.out.println("Cassandra connection is already closed");
			return;
		}

		System.out.println("Closing Cassandra connection...");
		session.close();
		System.out.println("Closing Cassandra connection... Done");
	}

	@Override
	public void createTables() {
		System.out.println("Creating tables...");
		
		session.execute(CQL_CREATE_ITEMS);
		System.out.println("Created table: " + TABLE_ITEMS);
		
		session.execute(CQL_CREATE_REVIEWS_BY_USER);
		System.out.println("Created table: " + TABLE_REVIEWS_BY_USER);
		
		session.execute(CQL_CREATE_REVIEWS_BY_ITEM);
		System.out.println("Created table: " + TABLE_REVIEWS_BY_ITEM);
		
		System.out.println("Creating tables... Done");
	}

	@Override
	public void initialize() {
		System.out.println("Initializing prepared statements...");
		
		pstmtInsertItem = session.prepare(CQL_INSERT_ITEM);
		pstmtInsertReviewByUser = session.prepare(CQL_INSERT_REVIEW_BY_USER);
		pstmtInsertReviewByItem = session.prepare(CQL_INSERT_REVIEW_BY_ITEM);
		pstmtSelectItem = session.prepare(CQL_SELECT_ITEM);
		pstmtSelectReviewsByUser = session.prepare(CQL_SELECT_REVIEWS_BY_USER);
		pstmtSelectReviewsByItem = session.prepare(CQL_SELECT_REVIEWS_BY_ITEM);
		
		System.out.println("Initializing prepared statements... Done");
	}

	@Override
	public void loadItems(String pathItemsFile) throws Exception {
		System.out.println("Loading items from: " + pathItemsFile);
		
		ExecutorService executor = Executors.newFixedThreadPool(250);
		BufferedReader reader = new BufferedReader(new FileReader(pathItemsFile));
		
		String line;
		int count = 0;
		
		while ((line = reader.readLine()) != null) {
			final String jsonLine = line;
			final int itemNum = count;
			
			executor.execute(new Runnable() {
				@Override
				public void run() {
					try {
						JSONObject json = new JSONObject(jsonLine);
						
						String asin = json.getString("asin");
						String title = json.optString("title", NOT_AVAILABLE_VALUE);
						String imageUrl = json.optString("imUrl", NOT_AVAILABLE_VALUE);
						String description = json.optString("description", NOT_AVAILABLE_VALUE);
						
						// Handle categories
						Set<String> categories = new TreeSet<>();
						if (json.has("categories")) {
							JSONArray categoriesArray = json.getJSONArray("categories");
							for (int i = 0; i < categoriesArray.length(); i++) {
								JSONArray categoryPath = categoriesArray.getJSONArray(i);
								for (int j = 0; j < categoryPath.length(); j++) {
									categories.add(categoryPath.getString(j));
								}
							}
						}
						if (categories.isEmpty()) {
							categories.add(NOT_AVAILABLE_VALUE);
						}
						
						BoundStatement bstmt = pstmtInsertItem.bind()
								.setString(0, asin)
								.setString(1, title)
								.setString(2, imageUrl)
								.setSet(3, categories, String.class)
								.setString(4, description);
						
						session.execute(bstmt);
						
						if (itemNum % 1000 == 0) {
							System.out.println("Loaded item: " + itemNum);
						}
					} catch (Exception e) {
						System.err.println("Error loading item: " + e.getMessage());
					}
				}
			});
			
			count++;
		}
		
		reader.close();
		executor.shutdown();
		executor.awaitTermination(1, TimeUnit.HOURS);
		
		System.out.println("Loading items... Done. Total items: " + count);
	}

	@Override
	public void loadReviews(String pathReviewsFile) throws Exception {
		System.out.println("Loading reviews from: " + pathReviewsFile);
		
		ExecutorService executor = Executors.newFixedThreadPool(250);
		BufferedReader reader = new BufferedReader(new FileReader(pathReviewsFile));
		
		String line;
		int count = 0;
		
		while ((line = reader.readLine()) != null) {
			final String jsonLine = line;
			final int reviewNum = count;
			
			executor.execute(new Runnable() {
				@Override
				public void run() {
					try {
						JSONObject json = new JSONObject(jsonLine);
					
						String reviewerId = json.getString("reviewerID");
						String asin = json.getString("asin");
						String reviewerName = json.optString("reviewerName", NOT_AVAILABLE_VALUE);
						double rating = json.optDouble("overall", -1.0);
						String summary = json.optString("summary", NOT_AVAILABLE_VALUE);
						String reviewText = json.optString("reviewText", NOT_AVAILABLE_VALUE);
						long unixReviewTime = json.optLong("unixReviewTime", 0);
						Instant reviewTime = Instant.ofEpochSecond(unixReviewTime);

						// Insert into reviews_by_user table
						BoundStatement bstmtUser = pstmtInsertReviewByUser.bind()
						.setString(0, reviewerId)
						.setInstant(1, reviewTime)
						.setString(2, asin)
						.setString(3, reviewerName)
						.setDouble(4, rating)
						.setString(5, summary)
						.setString(6, reviewText);

						session.execute(bstmtUser);

						// Insert into reviews_by_item table
						BoundStatement bstmtItem = pstmtInsertReviewByItem.bind()
						.setString(0, asin)
						.setInstant(1, reviewTime)
						.setString(2, reviewerId)
						.setString(3, reviewerName)
						.setDouble(4, rating)
						.setString(5, summary)
						.setString(6, reviewText);

						session.execute(bstmtItem);
						
						if (reviewNum % 10000 == 0) {
							System.out.println("Loaded review: " + reviewNum);
						}
					} catch (Exception e) {
						System.err.println("Error loading review: " + e.getMessage());
					}
				}
			});
			
			count++;
		}
		
		reader.close();
		executor.shutdown();
		executor.awaitTermination(1, TimeUnit.HOURS);
		
		System.out.println("Loading reviews... Done. Total reviews: " + count);
	}

	@Override
	public String item(String asin) {
		BoundStatement bstmt = pstmtSelectItem.bind().setString(0, asin);
		ResultSet rs = session.execute(bstmt);
		Row row = rs.one();
		
		if (row == null) {
			return "not exists";
		}
		
		String title = row.getString("title");
		String imageUrl = row.getString("image_url");
		Set<String> categories = row.getSet("categories", String.class);
		String description = row.getString("description");
		
		return formatItem(asin, title, imageUrl, categories, description);
	}

	@Override
	public Iterable<String> userReviews(String reviewerID) {
		ArrayList<String> reviewReprs = new ArrayList<>();
		
		BoundStatement bstmt = pstmtSelectReviewsByUser.bind().setString(0, reviewerID);
		ResultSet rs = session.execute(bstmt);
		
		int count = 0;
		for (Row row : rs) {
			Instant reviewTime = row.getInstant("review_time");
			String asin = row.getString("asin");
			String reviewerName = row.getString("reviewer_name");
			double rating = row.getDouble("rating");
			String summary = row.getString("summary");
			String reviewText = row.getString("review_text");

			String reviewRepr = formatReview(reviewTime, asin, reviewerID, reviewerName, rating, summary, reviewText);
			reviewReprs.add(reviewRepr);
			count++;
		}
		
		System.out.println("total reviews: " + count);
		return reviewReprs;
	}

	@Override
	public Iterable<String> itemReviews(String asin) {
		ArrayList<String> reviewReprs = new ArrayList<>();
		
		BoundStatement bstmt = pstmtSelectReviewsByItem.bind().setString(0, asin);
		ResultSet rs = session.execute(bstmt);
		
		int count = 0;
		for (Row row : rs) {
			Instant reviewTime = row.getInstant("review_time");
			String reviewerId = row.getString("reviewer_id");
			String reviewerName = row.getString("reviewer_name");
			double rating = row.getDouble("rating");
			String summary = row.getString("summary");
			String reviewText = row.getString("review_text");

			String reviewRepr = formatReview(reviewTime, asin, reviewerId, reviewerName, rating, summary, reviewText);
			reviewReprs.add(reviewRepr);
			count++;
		}
		
		System.out.println("total reviews: " + count);
		return reviewReprs;
	}

	// Formatting methods, do not change!
	private String formatItem(String asin, String title, String imageUrl, Set<String> categories, String description) {
		String itemDesc = "";
		itemDesc += "asin: " + asin + "\n";
		itemDesc += "title: " + title + "\n";
		itemDesc += "image: " + imageUrl + "\n";
		itemDesc += "categories: " + categories.toString() + "\n";
		itemDesc += "description: " + description + "\n";
		return itemDesc;
	}

	private String formatReview(Instant time, String asin, String reviewerId, String reviewerName, Double rating,
		String summary, String reviewText) {
		String reviewDesc = "time: " + time + ", asin: " + asin + ", reviewerID: " + reviewerId + ", reviewerName: "
		+ reviewerName + ", rating: " + rating + ", summary: " + summary + ", reviewText: " + reviewText + "\n";
		return reviewDesc;
		}

}