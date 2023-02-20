use sqlite::Value;

use crate::events::{Event, PaymentAuthorizedPayload};
use crate::projectors::Projector;

pub struct TotalAuthorizedProjector<'a> {
    connection: &'a sqlite::Connection,
}

impl<'a> TotalAuthorizedProjector<'a> {
    pub fn new(connection: &'a sqlite::Connection) -> Self {
        Self { connection }
    }
}

impl<'a> Projector for TotalAuthorizedProjector<'a> {
    fn project(&self, event: Event) -> Result<(), String> {
        match event {
            Event::PaymentAuthorized(PaymentAuthorizedPayload {
                amount,
                occurred_on,
                ..
            }) => {
                let mut statement = self
                .connection
                .prepare(
                    r"INSERT INTO total_authorized (amount, occurred_on) VALUES(:amount,:occurred_on)",
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
