use serde::{Deserialize, Serialize};

// ── Responses ────────────────────────────────────────────────────

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ObjectPropertiesResponse {
    #[serde(default)]
    pub metadata: Option<ObjectStoreProperties>,
    pub success: bool,
    #[serde(default)]
    pub errors: Vec<String>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ObjectStoreProperties {
    #[serde(default)]
    pub key: String,
    #[serde(default)]
    pub modified: String,
    #[serde(default)]
    pub created: String,
    #[serde(default)]
    pub size: i64,
    #[serde(default)]
    pub md5: Option<String>,
    #[serde(default)]
    pub mime: Option<String>,
    #[serde(default)]
    pub preview: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GetObjectResponse {
    #[serde(default)]
    pub job_id: Option<String>,
    /// Download URL — paste full URL (including params) into a browser.
    #[serde(default)]
    pub url: Option<String>,
    pub success: bool,
    #[serde(default)]
    pub errors: Vec<String>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ListObjectsResponse {
    #[serde(default)]
    pub path: String,
    #[serde(default)]
    pub objects: Vec<ObjectStoreSummary>,
    #[serde(default)]
    pub page: i64,
    #[serde(default)]
    pub total_pages: i64,
    #[serde(default)]
    pub object_storage_used: i64,
    #[serde(default)]
    pub object_storage_used_human: Option<String>,
    pub success: bool,
    #[serde(default)]
    pub errors: Vec<String>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ObjectStoreSummary {
    #[serde(default)]
    pub key: String,
    #[serde(default)]
    pub name: Option<String>,
    #[serde(default)]
    pub modified: String,
    #[serde(default)]
    pub mime: Option<String>,
    #[serde(default)]
    pub folder: bool,
    #[serde(default)]
    pub size: i64,
}

// ── Request helpers ──────────────────────────────────────────────

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ObjectKeyReq<'a> {
    pub organization_id: &'a str,
    pub key: &'a str,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ListObjectsReq<'a> {
    pub organization_id: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub path: Option<&'a str>,
}
