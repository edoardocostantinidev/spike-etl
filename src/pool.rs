use postgres::{Client, NoTls};
pub struct Pool;

impl Pool {
    pub fn get_client() -> Client {
        Client::connect(
            "host=localhost user=user password=password port=5432 connect_timeout=5",
            NoTls,
        )
        .unwrap()
    }
}
