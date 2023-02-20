use std::sync::{Arc, Mutex, MutexGuard};

use once_cell::sync::OnceCell;
use postgres::{Client, NoTls};
pub struct Pool;

impl Pool {
    pub fn get_client() -> MutexGuard<'static, Client> {
        static INSTANCE: OnceCell<Arc<Mutex<Client>>> = OnceCell::new();
        INSTANCE
            .get_or_init(|| {
                let client = Client::connect(
                    "host=localhost user=user password=password port=5432",
                    NoTls,
                )
                .unwrap();
                Arc::new(Mutex::new(client))
            })
            .lock()
            .unwrap()
    }
}
