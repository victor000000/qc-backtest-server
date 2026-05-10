use serde::Deserialize;
use serde_json::Value;

// ── Responses ────────────────────────────────────────────────────

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ProjectListResponse {
    #[serde(default)]
    pub projects: Vec<Project>,
    #[serde(default)]
    pub versions: Vec<LeanVersion>,
    pub success: bool,
    #[serde(default)]
    pub errors: Vec<String>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Project {
    pub project_id: i64,
    #[serde(default)]
    pub organization_id: String,
    pub name: String,
    #[serde(default)]
    pub modified: String,
    #[serde(default)]
    pub created: String,
    #[serde(default)]
    pub owner_id: i64,
    #[serde(default)]
    pub language: String,
    #[serde(default)]
    pub collaborators: Vec<Collaborator>,
    #[serde(default)]
    pub lean_version_id: Option<i64>,
    #[serde(default)]
    pub lean_pinned_to_master: Option<bool>,
    #[serde(default)]
    pub owner: Option<bool>,
    #[serde(default)]
    pub description: Option<String>,
    #[serde(default)]
    pub parameters: Vec<Value>,
    #[serde(default)]
    pub libraries: Vec<Value>,
    #[serde(default)]
    pub encrypted: Option<bool>,
    #[serde(default)]
    pub code_running: Option<bool>,
    #[serde(default)]
    pub lean_environment: Option<i64>,
    #[serde(default)]
    pub is_pinned: Option<bool>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Collaborator {
    pub uid: i64,
    pub live_control: bool,
    pub permission: String,
    #[serde(default)]
    pub public_id: String,
    #[serde(default)]
    pub profile_image: String,
    #[serde(default)]
    pub email: String,
    #[serde(default)]
    pub name: String,
    #[serde(default)]
    pub bio: String,
    #[serde(default)]
    pub owner: bool,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LeanVersion {
    pub id: i64,
    #[serde(default)]
    pub created: String,
    #[serde(default)]
    pub description: String,
    #[serde(default)]
    pub lean_hash: String,
    #[serde(default)]
    pub lean_cloud_hash: String,
    #[serde(default)]
    pub name: String,
    #[serde(default, rename = "ref")]
    pub ref_: String,
    #[serde(default)]
    pub public: Option<bool>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CollaboratorsResponse {
    #[serde(default)]
    pub collaborators: Vec<Collaborator>,
    pub success: bool,
    #[serde(default)]
    pub errors: Vec<String>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ReadCollaboratorsResponse {
    #[serde(default)]
    pub collaborators: Vec<Collaborator>,
    #[serde(default)]
    pub user_live_control: bool,
    #[serde(default)]
    pub user_permissions: String,
    pub success: bool,
    #[serde(default)]
    pub errors: Vec<String>,
}

// Node types in project_node.rs, request types in project_req.rs
