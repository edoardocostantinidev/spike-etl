use crate::events::Event;
use crate::events::ProductOrderedPayload;
use crate::pool::Pool;
use crate::projectors::Projector;

pub struct TotalOrderedProjector {}

impl TotalOrderedProjector {
    pub fn new() -> Self {
        Self {}
    }
}

impl Projector for TotalOrderedProjector {
    fn project(&self, event: Event) -> Result<(), String> {
        match event {
            Event::ProductOrdered(ProductOrderedPayload {
                amount,
                occurred_on,
                ..
            }) => Pool::get_client()
                .execute(
                    r"INSERT INTO total_ordered (amount, occurred_on) VALUES($1,$2)",
                    &[&amount, &occurred_on.to_string()],
                )
                .map(|_| ())
                .map_err(|e| e.to_string()),
            _ => Ok(()),
        }
    }
}
