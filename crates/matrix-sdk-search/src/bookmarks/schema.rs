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

use ruma::{
    MilliSecondsSinceUnixEpoch, OwnedUserId,
    events::room::message::{MessageType, OriginalSyncRoomMessageEvent},
};
use tantivy::{
    DateTime, TantivyDocument, doc,
    schema::{DateOptions, DateTimePrecision, Field, INDEXED, STORED, STRING, Schema, TEXT},
};

use crate::{
    bookmarks::BookmarkPointerInfo,
    error::{IndexError, IndexSchemaError},
};

pub(crate) trait MatrixBookmarkIndexSchema {
    fn new() -> Self;
    fn default_search_fields(&self) -> Vec<Field>;
    fn primary_key(&self) -> Field;
    fn deletion_key(&self) -> Field;
    fn target_event_id_key(&self) -> Field;
    fn target_room_id_key(&self) -> Field;
    fn as_tantivy_schema(&self) -> Schema;
    fn make_doc(
        &self,
        pointer_info: BookmarkPointerInfo,
        bookmark_content: IndexedBookmarkContent,
    ) -> Result<TantivyDocument, IndexError>;
}

#[derive(Debug, Clone)]
/// A struct that represents the fields of the original
/// bookmarked event that will be indexed.
pub struct IndexedBookmarkContent {
    /// Plain text content of the bookmarked event.
    /// The content of this field will be indexed.
    /// It cannot be empty otherwise the index won't
    /// save it.
    pub(super) body: String,
    /// Timestamp when the bookmarked event has been
    /// sent.
    pub(super) date: MilliSecondsSinceUnixEpoch,
    /// Matrix UserId of the sender of this bookmarked
    /// event.
    pub(super) sender: OwnedUserId,
}

impl IndexedBookmarkContent {
    /// Create a new IndexedBookmarkContent
    pub fn new(body: String, date: MilliSecondsSinceUnixEpoch, sender: OwnedUserId) -> Self {
        Self { body, date, sender }
    }
}

impl From<OriginalSyncRoomMessageEvent> for IndexedBookmarkContent {
    fn from(value: OriginalSyncRoomMessageEvent) -> Self {
        let body = match value.content.msgtype {
            MessageType::Audio(content) => {
                content.caption().map(ToOwned::to_owned).unwrap_or(content.filename().to_owned())
            }
            MessageType::Emote(content) => content.body,
            MessageType::File(content) => {
                content.caption().map(ToOwned::to_owned).unwrap_or(content.filename().to_owned())
            }
            #[cfg(feature = "unstable-msc4274")]
            MessageType::Gallery(content) => content.body,
            MessageType::Image(content) => {
                content.caption().map(ToOwned::to_owned).unwrap_or(content.filename().to_owned())
            }
            MessageType::Location(content) => content.body,
            MessageType::Notice(content) => content.body,
            MessageType::ServerNotice(content) => content.body,
            MessageType::Text(content) => content.body,
            MessageType::Video(content) => {
                content.caption().map(ToOwned::to_owned).unwrap_or(content.filename().to_owned())
            }
            // The index needs some body to allow indexing, so we fallback on the msgtype. This
            // is not ideal but this allows bookmarking any MessageLike timeline event.
            _message => _message.msgtype().to_owned(),
        };

        Self { body, date: value.origin_server_ts, sender: value.sender }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct BookmarkSchema {
    inner: Schema,
    /// The event id of this bookmark event (primary key).
    event_id_field: Field,
    /// The event id of the original version of the bookmark.
    /// Used by edits to refer to the event they edited (deletion key).
    original_event_id_field: Field,
    /// The event_id this bookmark references.
    /// It is also used as a key for editions and redactions.
    target_event_id_field: Field,
    /// The room_id in which the referenced event is.
    target_room_id_field: Field,
    body_field: Field,
    date_field: Field,
    sender_field: Field,
    default_search_fields: Vec<Field>,
}

impl MatrixBookmarkIndexSchema for BookmarkSchema {
    fn new() -> Self {
        let mut schema = Schema::builder();
        let event_id_field = schema.add_text_field("event_id", STORED | STRING);
        let original_event_id_field = schema.add_text_field("original_event_id", STORED | STRING);
        let target_event_id_field = schema.add_text_field("target_event_id", STORED | STRING);
        let target_room_id_field = schema.add_text_field("target_room_id", STORED | STRING);
        let body_field = schema.add_text_field("body", TEXT);

        let date_options =
            DateOptions::from(INDEXED).set_fast().set_precision(DateTimePrecision::Seconds);

        let date_field = schema.add_date_field("date", date_options);
        let sender_field = schema.add_text_field("sender", STRING);

        let default_search_fields = vec![body_field];

        let schema = schema.build();

        Self {
            inner: schema,
            event_id_field,
            original_event_id_field,
            target_event_id_field,
            target_room_id_field,
            body_field,
            date_field,
            sender_field,
            default_search_fields,
        }
    }

    fn default_search_fields(&self) -> Vec<Field> {
        self.default_search_fields.clone()
    }

    fn primary_key(&self) -> Field {
        self.event_id_field
    }

    fn deletion_key(&self) -> Field {
        self.original_event_id_field
    }

    fn target_event_id_key(&self) -> Field {
        self.target_event_id_field
    }

    fn target_room_id_key(&self) -> Field {
        self.target_room_id_field
    }

    fn as_tantivy_schema(&self) -> Schema {
        self.inner.clone()
    }

    /// Given a [`BookmarkPointerInfo`] and a [`IndexedBookmarkContent`]
    /// return a [`TantivyDocument`].
    fn make_doc(
        &self,
        pointer_info: BookmarkPointerInfo,
        bookmark_content: IndexedBookmarkContent,
    ) -> Result<TantivyDocument, IndexError> {
        let document = doc!(
            self.event_id_field => pointer_info.event_id.to_string(),
            self.original_event_id_field => pointer_info.original_event_id.to_string(),
            self.target_event_id_field => pointer_info.target_event_id.to_string(),
            self.target_room_id_field => pointer_info.target_room_id.to_string(),
            self.body_field => bookmark_content.body,
            self.date_field =>
                DateTime::from_timestamp_millis(
                    bookmark_content.date.get().into()),
            self.sender_field => bookmark_content.sender.to_string(),
        );

        Ok(document)
    }
}

impl TryFrom<Schema> for BookmarkSchema {
    type Error = IndexSchemaError;

    fn try_from(schema: Schema) -> Result<BookmarkSchema, Self::Error> {
        let event_id_field = schema.get_field("event_id")?;
        let original_event_id_field = schema.get_field("original_event_id")?;
        let target_event_id_field = schema.get_field("target_event_id")?;
        let target_room_id_field = schema.get_field("target_room_id")?;
        let body_field = schema.get_field("body")?;
        let date_field = schema.get_field("date")?;
        let sender_field = schema.get_field("sender")?;

        let default_search_fields = vec![body_field];

        Ok(Self {
            inner: schema,
            event_id_field,
            original_event_id_field,
            target_event_id_field,
            body_field,
            date_field,
            sender_field,
            target_room_id_field,
            default_search_fields,
        })
    }
}
