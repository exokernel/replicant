use std::time::Instant;

use anyhow::Result;
use automerge::{
    AutoCommit, AutomergeError, ObjId, Prop, ReadDoc, ScalarValue, Value,
    sync::{self, SyncDoc},
    transaction::Transactable,
};

pub struct InstrumentedDoc {
    inner: AutoCommit,
}

impl InstrumentedDoc {
    pub fn new() -> Self {
        Self {
            inner: AutoCommit::new(),
        }
    }

    pub fn put<O, P, V>(&mut self, obj: O, prop: P, value: V) -> Result<()>
    where
        O: AsRef<ObjId>,
        P: Into<Prop>,
        V: Into<ScalarValue>,
    {
        let start = Instant::now();
        self.inner.put(obj, prop, value)?;
        let duration_us = start.elapsed().as_micros();
        tracing::info!(duration_us, "apply_changes");
        Ok(())
    }

    pub fn get<O: AsRef<ObjId>, P: Into<Prop>>(
        &self,
        obj: O,
        prop: P,
    ) -> Result<Option<(Value<'_>, ObjId)>, AutomergeError> {
        self.inner.get(obj, prop)
    }

    pub fn generate_sync_message(
        &mut self,
        sync_state: &mut sync::State,
    ) -> Option<sync::Message> {
        let start = Instant::now();
        let msg = self.inner.sync().generate_sync_message(sync_state);
        let duration_us = start.elapsed().as_micros();
        tracing::info!(duration_us, "generate_sync_message");
        msg
    }

    pub fn receive_sync_message(
        &mut self,
        sync_state: &mut sync::State,
        msg: sync::Message,
    ) -> Result<()> {
        let start = Instant::now();
        self.inner.sync().receive_sync_message(sync_state, msg)?;
        let duration_us = start.elapsed().as_micros();
        tracing::info!(duration_us, "receive_sync_message");
        Ok(())
    }

    pub fn get_heads(&mut self) -> Vec<automerge::ChangeHash> {
        self.inner.get_heads()
    }

    pub fn save(&mut self) -> Vec<u8> {
        let start = Instant::now();
        let bytes = self.inner.save();
        let duration_us = start.elapsed().as_micros();
        let size_bytes = bytes.len();
        tracing::info!(duration_us, size_bytes, "save");
        bytes
    }
}
