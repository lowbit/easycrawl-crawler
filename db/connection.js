import pg from "pg";
import dotenv from "dotenv";

const { Pool } = pg;
dotenv.config();

const pool = new Pool({
  user: process.env.DB_USER,
  host: process.env.DB_HOST,
  database: process.env.DB_NAME,
  password: process.env.DB_PASSWORD,
  port: process.env.DB_PORT,
  // ssl: {
  //     rejectUnauthorized: false // For development - consider enabling for production
  // },
  // Connection handling parameters
  max: 20, // Maximum number of clients in the pool
  idleTimeoutMillis: 30000, // Close idle clients after 30 seconds
  connectionTimeoutMillis: 10000, // Return an error after 10 seconds if connection could not be established
  maxUses: 7500, // Close a connection after it has been used 7500 times
});

// Add error handler for the pool
pool.on("error", (err, client) => {
  console.error("Unexpected error on idle client", err);
});

// Function to test the database connection
export async function testDatabaseConnection() {
  try {
    const client = await pool.connect();
    await client.query("SELECT 1");
    client.release();
    console.log("Database connection successful");
    return true;
  } catch (err) {
    console.error("Error connecting to the database:", err);
    return false;
  }
}

// Wrapper function for database queries with retry logic
export async function executeWithRetry(operation, maxRetries = 3) {
  let lastError;
  for (let i = 0; i < maxRetries; i++) {
    try {
      const client = await pool.connect();
      try {
        return await operation(client);
      } finally {
        client.release();
      }
    } catch (err) {
      lastError = err;
      console.error(`Attempt ${i + 1} failed:`, err.message);
      if (i < maxRetries - 1) {
        const delay = Math.min(1000 * Math.pow(2, i), 10000); // Exponential backoff, max 10 seconds
        await new Promise((resolve) => setTimeout(resolve, delay));
      }
    }
  }
  throw lastError;
}

export { pool };
