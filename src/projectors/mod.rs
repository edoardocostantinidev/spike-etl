use crate::events::Event;

pub mod total_authorized_projector;
pub mod total_collected_projector;
pub mod total_ordered_projector;
pub trait Projector {
    fn project(&self, event: Event) -> Result<(), String>;
}
