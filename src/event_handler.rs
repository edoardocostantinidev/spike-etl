use std::fmt::Display;

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

impl Display for EventError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            EventError::UnknownEvent(s) => f.write_fmt(format_args!("Unknown Error: {s}")),
            EventError::ProjectionError(s) => f.write_fmt(format_args!("Projection Error: {s}")),
            EventError::ReconcilationEngineError(s) => {
                f.write_fmt(format_args!("Reconciliation Engine Error: {s}"))
            }
        }
    }
}

pub struct EventHandler {
    projectors: Vec<Box<dyn Projector>>,
    reconciliation_engine: ReconciliationEngine,
}

impl EventHandler {
    pub fn new() -> Self {
        Self {
            projectors: vec![
                Box::new(TotalOrderedProjector::new()),
                Box::new(TotalAuthorizedProjector::new()),
                Box::new(TotalCollectedProjector::new()),
            ],
            reconciliation_engine: ReconciliationEngine::new(),
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
            .map_err(|err| EventError::ReconcilationEngineError(err.to_string()))?;

        Ok(())
    }
}
