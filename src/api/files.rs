use anyhow::Result;

use crate::client::QcClient;
use crate::models::common::RestResponse;
use crate::models::file::{
    CreateFileReq, DeleteFileReq, PatchFileReq, ProjectFilesResponse, ReadFilesReq,
    UpdateFileContentsReq, UpdateFileNameReq,
};

impl QcClient {
    /// Create a file in a project.
    ///
    /// # Errors
    /// Returns `Err` on HTTP transport failure or non-2xx response.
    pub async fn create_file(
        &self,
        project_id: i64,
        name: &str,
        content: Option<&str>,
    ) -> Result<RestResponse> {
        self.post(
            "/files/create",
            &CreateFileReq {
                project_id,
                name,
                content,
                code_source_id: None,
            },
        )
        .await
    }

    /// Read files in a project.
    ///
    /// # Errors
    /// Returns `Err` on HTTP transport failure or deserialization failure.
    pub async fn read_files(
        &self,
        project_id: i64,
        name: Option<&str>,
    ) -> Result<ProjectFilesResponse> {
        self.post(
            "/files/read",
            &ReadFilesReq {
                project_id,
                name,
                code_source_id: None,
            },
        )
        .await
    }

    /// Rename a file.
    ///
    /// # Errors
    /// Returns `Err` on HTTP transport failure or non-2xx response.
    pub async fn update_file_name(
        &self,
        project_id: i64,
        name: &str,
        new_name: &str,
    ) -> Result<RestResponse> {
        self.post(
            "/files/update",
            &UpdateFileNameReq {
                project_id,
                name,
                new_name,
                code_source_id: None,
            },
        )
        .await
    }

    /// Upload file contents.
    ///
    /// # Errors
    /// Returns `Err` on HTTP transport failure or non-2xx response.
    pub async fn update_file_contents(
        &self,
        project_id: i64,
        name: &str,
        content: &str,
    ) -> Result<RestResponse> {
        self.post(
            "/files/update",
            &UpdateFileContentsReq {
                project_id,
                name,
                content,
                code_source_id: None,
            },
        )
        .await
    }

    /// Delete a file from a project.
    ///
    /// # Errors
    /// Returns `Err` on HTTP transport failure or non-2xx response.
    pub async fn delete_file(&self, project_id: i64, name: &str) -> Result<RestResponse> {
        self.post(
            "/files/delete",
            &DeleteFileReq {
                project_id,
                name,
                code_source_id: None,
            },
        )
        .await
    }

    /// Apply a patch to a project file.
    ///
    /// # Errors
    /// Returns `Err` on HTTP transport failure or non-2xx response.
    pub async fn patch_file(&self, project_id: i64, patch: &str) -> Result<RestResponse> {
        self.post(
            "/files/patch",
            &PatchFileReq {
                project_id,
                patch,
                code_source_id: None,
            },
        )
        .await
    }
}
