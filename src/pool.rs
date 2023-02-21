use lazy_static::lazy_static;
use postgres::NoTls;
use r2d2_postgres::{
    r2d2::{self, PooledConnection},
    PostgresConnectionManager,
};

/*
   bank transactions ->
       UPSERT transaction_id#000 VALUE amount
   product ordered ->
       UPSERT order_id#000 VALUE amount
   payment collected ->
       UPSERT payment_id#000 VALUE amount
       UPSERT transaction_id#000 VALUE relative_payment=payment_id#000
   payment authorized ->
       UPSERT payment_id#000 VALUE amount relative_order=order_id#000
       UPSERT order_id#000 VALUE amount relative_payment=payment_id#000

    reconcile:
    bank transactions -> GET FROM transaction_id#000 relative_payment -> GET FROM payment_id#000 relative_order=order_id#000 -> GET FROM order_id#000
    product ordered ->
    payment collected ->
    payment authorized ->



    RELATIONS
    (t_id,o_id,p_id)   -> completo
    bank transactions  -> cerco t_id in relations, se non c'è scrivo t_id in relations
    product ordered    -> cerco o_id in relations, se non c'è scrivo o_id in relations
    payment collected  -> cerco t_id/p_id in relations -> aggiungo info mancante / scrivo t_id e p_id in relations
    payment authorized -> cerco o_id/p_id in relations -> aggiungo info mancante / scrivo p_id e o_id in relations

    quando riconcilio:
    cerco per chiave naturale su relations e faccio i confronti necessari.
*/

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
        DROP TABLE IF EXISTS relations;
        
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
            ordered_amount double precision default 0,
            occurred_on text
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
        );
        
        CREATE TABLE relations (
            id BIGSERIAL PRIMARY KEY,
            payment_id text default null,
            order_id text default null,
            transaction_id text default null
        );

        CREATE INDEX t_id_idx ON relations(transaction_id);
        CREATE INDEX p_id_idx ON relations(payment_id);
        CREATE INDEX o_id_idx ON relations(order_id);
        ";

    queries.split(";").filter(|s| !s.is_empty()).for_each(|q| {
        client.execute(q, &[]).map(|_| ()).unwrap();
    });
}
