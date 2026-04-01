use std::time::Instant;

use anyhow::Result;
use automerge::{
    AutoCommit, AutomergeError, ObjId, Prop, ReadDoc, ScalarValue, Value,
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

    pub fn save(&mut self) -> Vec<u8> {
        let start = Instant::now();
        let bytes = self.inner.save();
        let duration_us = start.elapsed().as_micros();
        let size_bytes = bytes.len();
        tracing::info!(duration_us, size_bytes, "save");
        bytes
    }
}
