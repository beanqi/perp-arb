use crate::engine::types::TradeCommand;

#[derive(Clone, Debug)]
pub struct TradeExecRuntime {
    pub command_queue_bound: usize,
}

impl Default for TradeExecRuntime {
    fn default() -> Self {
        Self {
            command_queue_bound: 1024,
        }
    }
}

impl TradeExecRuntime {
    pub fn accepts(&self, _: &TradeCommand) -> bool {
        true
    }
}
