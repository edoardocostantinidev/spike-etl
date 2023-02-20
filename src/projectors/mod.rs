use crate::events::Event;

pub mod total_ordered_projector;

pub trait Projector {
    fn project(&self, event: Event) -> Result<(), String> {
        todo!()
    }
}
