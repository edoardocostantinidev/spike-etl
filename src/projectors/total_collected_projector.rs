use crate::events::BankTransactionIssuedPayload;
use crate::events::Event;
use crate::pool::Pool;
use crate::projectors::Projector;

pub struct TotalCollectedProjector {}

impl TotalCollectedProjector {
    pub fn new() -> Self {
        Self {}
    }
}

impl Projector for TotalCollectedProjector {
    fn project(&self, event: Event) -> Result<(), String> {
        match event {
            Event::BankTransactionIssued(BankTransactionIssuedPayload {
                amount,
                occurred_on,
                ..
            }) => Pool::get_client()
                .execute(
                    r"INSERT INTO total_collected (amount, occurred_on) VALUES($1,$2)",
                    &[&amount, &occurred_on.to_string()],
                )
                .map_err(|e| e.to_string())
                .map(|_| ()),
            _ => Ok(()),
        }
    }
}
