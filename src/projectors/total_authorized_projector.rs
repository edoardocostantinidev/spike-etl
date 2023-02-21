use crate::events::{Event, PaymentAuthorizedPayload};
use crate::projectors::Projector;

pub struct TotalAuthorizedProjector {}

impl TotalAuthorizedProjector {
    pub fn new() -> Self {
        Self {}
    }
}

impl Projector for TotalAuthorizedProjector {
    fn project(&self, event: Event) -> Result<(), String> {
        match event {
            Event::PaymentAuthorized(PaymentAuthorizedPayload {
                amount,
                occurred_on,
                ..
            }) => crate::pool::POOL
                .get()
                .unwrap()
                .execute(
                    r"INSERT INTO total_authorized (amount, occurred_on) VALUES($1,$2)",
                    &[&amount, &occurred_on.to_string()],
                )
                .map_err(|e| e.to_string())
                .map(|_| ()),
            _ => Ok(()),
        }
    }
}
