import pg from 'pg';
import dotenv from 'dotenv';
import format from 'pg-format';

const { Pool } = pg;
dotenv.config();

const pool = new Pool({
    user: process.env.DB_USER,
    host: process.env.DB_HOST,
    database: process.env.DB_NAME,
    password: process.env.DB_PASSWORD,
    port: process.env.DB_PORT
});

export async function insertItems(config_code, items) {
    const client = await pool.connect();
    console.log('db connection opened');
    try {
        await client.query('BEGIN');
        const batchSize = process.env.BATCH_SIZE;
        const numBatches = Math.ceil(items.length / batchSize);

        for (let i = 0; i < numBatches; i++) {
            const start = i * batchSize;
            const end = Math.min((i + 1) * batchSize, items.length);
            const batchItems = items.slice(start, end);
            const insertValues = batchItems.map((item) => [
                config_code,
                item.title,
                item.link,
                item.price,
                item.priceraw
            ]);
            if (insertValues.length === 0) {
                // No items to insert
                console.log('No items to insert');
                continue;
            }
            console.log('inserting data to db, start:', start, ', end:', end, ', full size:', items.length);
            await client.query(format('INSERT INTO crawler_raw (config_code, title, link, price_string, price) VALUES %L', insertValues));
            console.log('inserted data to db, start:', start, ', end:', end, ', full size:', items.length);
        }
        await client.query('COMMIT');
    } catch (err) {
        await client.query('ROLLBACK');
        throw err;
    } finally {
        client.release();
        console.log('db connection closed');
    }
}
export async function insertCrawlError(jobId, website, category, err) {
    const client = await pool.connect();
    console.log('db connection opened');
    try {
        await client.query('BEGIN');
        const errorDetails = [website, category, err.message, new Date(), jobId];
        console.log('Inserting error details into database:', errorDetails);
        await client.query('INSERT INTO crawler_error (website, category, error, created, job_id) VALUES ($1, $2, $3, $4, $5)', errorDetails);
        await client.query('COMMIT');
    } catch (err) {
        console.log('Error inserting error details into database:', err);
    } finally {
        client.release();
        console.log('db connection closed');
    }
}
export async function processCrawlJobs() {
    const client = await pool.connect();
    console.log('db connection opened');
    try {
        //Search for awaiting jobs that do not have already running jobs on the same host
        const awaitingCrawlJobsQuery = await client.query('select * from crawler_job where status = $1 and website_code not in (select website_code from crawler_job where status = $2 limit 1) order by id desc limit 1', ['Waiting execution', 'Running']);
        if (awaitingCrawlJobsQuery.rowCount > 0) {
            const job = awaitingCrawlJobsQuery.rows[0];
            console.log(job);
            const crawlerConfigQuery = await client.query('select * from crawler_config where code = $1', [job.config_code]);
            if(crawlerConfigQuery.rowCount>0){
                var crawlerConfig = crawlerConfigQuery.rows[0];
                crawlerConfig.job_id = job.id;
                await client.query('update crawler_job set status = $1, started_at = now(), modified = now(), modified_by = $3 where id = $2',['Running',job.id,'SYSTEM']);
                return crawlerConfig;
            }
        }
    } catch (err) {
        console.log('Error processing crawl jobs:', err);
    } finally {
        client.release();
        console.log('db connection closed');
    }
}
export async function finishCrawlJob(id) {
    const client = await pool.connect();
    console.log('db connection opened');
    try {
        await client.query('update crawler_job set status = $1, finished_at = now(), modified = now(), modified_by = $3 where id = $2',['Finished',id,'SYSTEM']);
    } catch (err) {
        console.log('Error processing crawl jobs:', err);
    } finally {
        client.release();
        console.log('db connection closed');
    }
}

export async function failCrawlJob(id) {
    const client = await pool.connect();
    console.log('db connection opened');
    try {
        await client.query('update crawler_job set status = $1, finished_at = now(), modified = now(), modified_by = $3 where id = $2',['Failed',id,'SYSTEM']);
    } catch (err) {
        console.log('Error processing crawl jobs:', err);
    } finally {
        client.release();
        console.log('db connection closed');
    }
}