use serde::{Deserialize, Serialize};

/// The `_revisions` field returned by `CouchDB` when a document is requested with
/// `revs=true`. It encodes the revision-hash ancestry of a single revision as a
/// starting generation plus the list of revision hashes (newest first).
///
/// Because the revision is addressed explicitly, this can be retrieved for any
/// revision — including a deletion tombstone — even though a plain lookup of a
/// deleted document returns `404 not_found`.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Revisions {
    /// Generation number of the newest revision in [`Revisions::ids`].
    pub start: u64,
    /// Revision hashes, newest first. The full revision string for `ids[i]` is
    /// `{start - i}-{ids[i]}`.
    pub ids: Vec<String>,
}

impl Revisions {
    /// Full revision strings (`{generation}-{hash}`), newest first. Index 0 is
    /// the revision the history was requested for; index 1 is its parent, etc.
    #[must_use]
    pub fn revision_ids(&self) -> Vec<String> {
        self.ids
            .iter()
            .enumerate()
            .map(|(i, hash)| format!("{}-{}", self.start - i as u64, hash))
            .collect()
    }

    /// The parent (previous) revision string, if one exists.
    #[must_use]
    pub fn parent(&self) -> Option<String> {
        let hash = self.ids.get(1)?;
        Some(format!("{}-{}", self.start - 1, hash))
    }
}

/// Wrapper used to deserialize the `_revisions` field from a document response.
#[derive(Deserialize)]
pub(crate) struct RevisionsEnvelope {
    #[serde(rename = "_revisions")]
    pub revisions: Revisions,
}
