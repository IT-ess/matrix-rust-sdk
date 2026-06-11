// Copyright 2026 The Matrix.org Foundation C.I.C.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//! A module for building a [`BookmarkIndex`]

use std::{fs, path::PathBuf, sync::Arc};

use tantivy::{
    Index,
    directory::{MmapDirectory, error::OpenDirectoryError},
};
use zeroize::Zeroizing;

use crate::{
    bookmarks::{
        BookmarkIndex,
        schema::{BookmarkSchema, MatrixBookmarkIndexSchema},
    },
    encrypted::encrypted_dir::{EncryptedMmapDirectory, PBKDF_COUNT},
    error::IndexError,
};

/// Builder for [`BookmarkIndex`].
pub struct BookmarkIndexBuilder {}

impl BookmarkIndexBuilder {
    /// Make an index on disk
    pub fn new_on_disk(path: PathBuf) -> PhysicalBookmarkIndexBuilder {
        PhysicalBookmarkIndexBuilder::new(path)
    }

    /// Make an index in memory
    pub fn new_in_memory() -> MemoryBookmarkIndexBuilder {
        MemoryBookmarkIndexBuilder::new()
    }
}

/// Incomplete builder for [`BookmarkIndex`] on disk.
pub struct PhysicalBookmarkIndexBuilder {
    path: PathBuf,
}

impl PhysicalBookmarkIndexBuilder {
    /// Make an new [`PhysicalBookmarkIndexBuilder`]
    pub(crate) fn new(path: PathBuf) -> PhysicalBookmarkIndexBuilder {
        PhysicalBookmarkIndexBuilder { path }
    }

    /// Make an unencrypted index
    pub fn unencrypted(&self) -> UnencryptedPhysicalBookmarkIndexBuilder {
        UnencryptedPhysicalBookmarkIndexBuilder { path: self.path.clone() }
    }

    /// Make an encrypted index
    pub fn encrypted<P: Into<String>>(&self, password: P) -> EncryptedPhysicalBookmarkIndexBuilder {
        EncryptedPhysicalBookmarkIndexBuilder {
            path: self.path.clone(),
            password: Zeroizing::new(password.into()),
        }
    }
}

/// Complete builder for [`BookmarkIndex`] on disk.
pub struct UnencryptedPhysicalBookmarkIndexBuilder {
    path: PathBuf,
}

impl UnencryptedPhysicalBookmarkIndexBuilder {
    /// Build the [`BookmarkIndex`]
    pub fn build(&self) -> Result<BookmarkIndex, IndexError> {
        let path = self.path.join("bookmarks_unencrypted");
        let mmap_dir = match MmapDirectory::open(path) {
            Ok(dir) => Ok(dir),
            Err(err) => match err {
                OpenDirectoryError::DoesNotExist(path) => {
                    fs::create_dir_all(path.clone()).map_err(|err| {
                        OpenDirectoryError::IoError {
                            io_error: Arc::new(err),
                            directory_path: path.to_path_buf(),
                        }
                    })?;
                    MmapDirectory::open(path)
                }
                _ => Err(err),
            },
        }?;
        let schema = BookmarkSchema::new();
        let index = Index::open_or_create(mmap_dir, schema.as_tantivy_schema())?;
        Ok(BookmarkIndex::new_with(index, schema))
    }
}

/// Complete builder for [`BookmarkIndex`] on disk.
pub struct EncryptedPhysicalBookmarkIndexBuilder {
    path: PathBuf,
    password: Zeroizing<String>,
}

impl EncryptedPhysicalBookmarkIndexBuilder {
    /// Build the [`BookmarkIndex`]
    pub fn build(&self) -> Result<BookmarkIndex, IndexError> {
        let path = self.path.join("bookmarks_encrypted");
        let mmap_dir =
            match EncryptedMmapDirectory::open_or_create(path, &self.password, PBKDF_COUNT) {
                Ok(dir) => Ok(dir),
                Err(err) => match err {
                    OpenDirectoryError::DoesNotExist(path) => {
                        fs::create_dir_all(path.clone()).map_err(|err| {
                            OpenDirectoryError::IoError {
                                io_error: Arc::new(err),
                                directory_path: path.to_path_buf(),
                            }
                        })?;
                        EncryptedMmapDirectory::open_or_create(path, &self.password, PBKDF_COUNT)
                    }
                    _ => Err(err),
                },
            }?;
        let schema = BookmarkSchema::new();
        let index = Index::open_or_create(mmap_dir, schema.as_tantivy_schema())?;
        Ok(BookmarkIndex::new_with(index, schema))
    }
}

/// Builder for [`BookmarkIndex`] in memory
pub struct MemoryBookmarkIndexBuilder {}

impl MemoryBookmarkIndexBuilder {
    /// Make an new [`MemoryIndexBuilder`]
    pub(crate) fn new() -> MemoryBookmarkIndexBuilder {
        MemoryBookmarkIndexBuilder {}
    }

    /// Build the [`BookmarkIndex`]
    pub fn build(&self) -> BookmarkIndex {
        let schema = BookmarkSchema::new();
        let index = Index::create_in_ram(schema.as_tantivy_schema());
        BookmarkIndex::new_with(index, schema)
    }
}
