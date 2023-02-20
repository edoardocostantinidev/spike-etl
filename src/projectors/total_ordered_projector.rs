use sqlite::Value;

use crate::events::Event;
use crate::events::ProductOrderedPayload;
use crate::projectors::Projector;

pub struct TotalOrderedProjector<'a> {
    connection: &'a sqlite::Connection,
}

impl<'a> TotalOrderedProjector<'a> {
    pub fn new(connection: &'a sqlite::Connection) -> Self {
        Self { connection }
    }
}

impl<'a> Projector for TotalOrderedProjector<'a> {
    fn project(&self, event: Event) -> Result<(), String> {
        match event {
            Event::ProductOrdered(ProductOrderedPayload {
                amount,
                occurred_on,
                ..
            }) => {
                let mut statement = self
                .connection
                .prepare(
                    r"INSERT INTO total_ordered (amount, occurred_on) VALUES(:amount,:occurred_on)",
                )
                .unwrap();
                statement
                    .bind::<&[(_, Value)]>(&[
                        (":amount", amount.into()),
                        (":occurred_on", occurred_on.to_string().into()),
                    ])
                    .map_err(|e| e.to_string())?;

                statement.next().map_err(|e| e.to_string()).map(|_| ())
            }
            _ => Ok(()),
        }
    }
}
