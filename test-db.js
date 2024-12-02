import pg from 'pg';
import dotenv from 'dotenv';

dotenv.config();

async function testConnection() {
    const pool = new pg.Pool({
        user: process.env.DB_USER,
        host: process.env.DB_HOST,
        database: process.env.DB_NAME,
        password: process.env.DB_PASSWORD,
        port: process.env.DB_PORT,
        ssl: {
            rejectUnauthorized: false // For development - adjust for production
        },
        // Connection handling parameters
        connectionTimeoutMillis: 10000, // 10 seconds
        idleTimeoutMillis: 30000,
        max: 20
    });

    try {
        console.log('\nAttempting to connect to database...');
        const client = await pool.connect();
        console.log('Successfully connected to database!');

        const result = await client.query('SELECT NOW()');
        console.log('Current database time:', result.rows[0].now);

        client.release();
        await pool.end();
    } catch (err) {
        console.error('Error connecting to database:', err);
        console.error('Full error:', err.stack);

        // Additional connection diagnostics
        if (err.code) {
            console.log('Error code:', err.code);
        }
    }
}

console.log('Database connection settings:');
console.log('Host:', process.env.DB_HOST);
console.log('Port:', process.env.DB_PORT);
console.log('Database:', process.env.DB_NAME);
console.log('User:', process.env.DB_USER);
console.log('Password length:', process.env.DB_PASSWORD?.length);

testConnection();