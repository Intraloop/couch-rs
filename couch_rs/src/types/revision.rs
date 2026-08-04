use crate::types::document::DocumentId;
use serde::{Deserialize, Serialize};

/// Represents the status of a document revision
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum RevisionStatus {
    /// Revision is available
    Available,
    /// Revision has been deleted
    Deleted,
    /// Revision is missing
    Missing,
}

/// Information about a specific document revision
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct RevisionInfo {
    /// The revision identifier
    pub rev: String,
    /// The status of this revision
    pub status: RevisionStatus,
}

/// Response structure for document revision information
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct DocumentRevisions {
    /// Document ID
    #[serde(rename = "_id")]
    pub id: DocumentId,
    /// Current revision
    #[serde(rename = "_rev")]
    pub rev: String,
    /// List of revision information
    #[serde(rename = "_revs_info")]
    pub revs_info: Vec<RevisionInfo>,
}
