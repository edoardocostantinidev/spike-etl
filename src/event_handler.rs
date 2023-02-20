use crate::events::*;
use crate::projectors::total_authorized_projector::TotalAuthorizedProjector;
use crate::projectors::total_collected_projector::TotalCollectedProjector;
use crate::projectors::total_ordered_projector::TotalOrderedProjector;
use crate::projectors::Projector;
use crate::reconciliation_engine::ReconciliationEngine;

#[derive(Debug)]
pub enum EventError {
    UnknownEvent(String),
    ProjectionError(String),
    ReconcilationEngineError(String),
}

pub struct EventHandler<'a> {
    projectors: Vec<Box<dyn Projector + 'a>>,
    reconciliation_engine: ReconciliationEngine<'a>,
}

impl<'a> EventHandler<'a> {
    pub fn new(conn: &'a sqlite::Connection) -> Self {
        Self {
            projectors: vec![
                Box::new(TotalOrderedProjector::new(conn)),
                Box::new(TotalAuthorizedProjector::new(conn)),
                Box::new(TotalCollectedProjector::new(conn)),
            ],
            reconciliation_engine: ReconciliationEngine::new(conn),
        }
    }
    pub fn accept(&self, event: Event) -> Result<(), EventError> {
        self.projectors
            .iter()
            .map(|p| {
                p.project(event.clone())
                    .map_err(|e| EventError::ProjectionError(e))
            })
            .collect::<Result<Vec<_>, _>>()?;

        self.reconciliation_engine
            .reconcile(event.clone())
            .map_err(|err| EventError::ReconcilationEngineError(err))?;

        Ok(())
    }
}
