use lazy_static::lazy_static;
use postgres::NoTls;
use r2d2_postgres::{r2d2, PostgresConnectionManager};

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
