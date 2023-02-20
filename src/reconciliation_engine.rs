use crate::events::Event;

pub struct ReconciliationEngine<'a> {
    pub connection: &'a sqlite::Connection,
}

impl<'a> ReconciliationEngine<'a> {
    pub fn new(connection: &'a sqlite::Connection) -> Self {
        Self { connection }
    }

    pub fn reconcile(&self, event: Event) -> Result<(), String> {
        Ok(())
    }
}
