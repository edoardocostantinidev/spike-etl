use lazy_static::lazy_static;
use postgres::NoTls;
use r2d2_postgres::{
    r2d2::{self, PooledConnection},
    PostgresConnectionManager,
};

lazy_static! {
    pub static ref POOL: r2d2::Pool<PostgresConnectionManager<NoTls>> = {
        let manager = PostgresConnectionManager::new(
            "host=localhost user=user password=password port=5432 connect_timeout=5"
                .parse()
                .unwrap(),
            NoTls,
        );
        r2d2::Pool::new(manager).unwrap()
    };
}

type Client = PooledConnection<PostgresConnectionManager<NoTls>>;

pub fn reset_db(client: &mut Client) {
    let queries = r"
        DROP TABLE IF EXISTS total_ordered;
        DROP TABLE IF EXISTS total_authorized;
        DROP TABLE IF EXISTS total_collected;
        DROP TABLE IF EXISTS bank_transactions;
        DROP TABLE IF EXISTS payment_authorizations;
        DROP TABLE IF EXISTS payment_collections;
        DROP TABLE IF EXISTS product_orders;
        
        CREATE TABLE total_ordered (
            id SERIAL PRIMARY KEY,
            amount double precision,
            occurred_on text
        );

        CREATE TABLE total_authorized (
            id SERIAL PRIMARY KEY,
            amount double precision,
            occurred_on text
        );

        CREATE TABLE total_collected (
            id SERIAL PRIMARY KEY,
            amount double precision,
            occurred_on text
        );
        
        CREATE TABLE bank_transactions (
            transaction_id text PRIMARY KEY,
            amount double precision,
            occurred_on text,
            reconciled int4 default 0
        );

        CREATE TABLE payment_authorizations (
            payment_id text,
            order_id text,
            amount double precision,
            occurred_on text,
            PRIMARY KEY (order_id, payment_id)
        );

        CREATE TABLE payment_collections (
            payment_id text,
            transaction_id text,
            amount double precision,
            occurred_on text,
            PRIMARY KEY (transaction_id, payment_id)
        );

        CREATE TABLE product_orders (
            order_id text PRIMARY KEY,
            amount double precision,
            collected_amount double precision default 0,
            occurred_on text,
            insurance_code text,
            installment_type text,
            event_type text
        );";

    queries.split(";").filter(|s| !s.is_empty()).for_each(|q| {
        client.execute(q, &[]).map(|_| ()).unwrap();
    });
}
