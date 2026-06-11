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

use ruma::{MilliSecondsSinceUnixEpoch, OwnedEventId, OwnedUserId};
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
    fn pointer_event_id_key(&self) -> Field;
    fn sender_key(&self) -> Field;
    fn room_id_key(&self) -> Field;
    fn body_key(&self) -> Field;
    fn date_key(&self) -> Field;
    fn get_field_name(&self, field: Field) -> &str;
    fn as_tantivy_schema(&self) -> Schema;
    fn make_doc(
        &self,
        pointer_info: BookmarkPointerInfo,
        bookmark_content: BookmarkContent,
    ) -> Result<TantivyDocument, IndexError>;
}

#[derive(Debug, Clone)]
/// A struct that represents the fields of the original
/// event that will be stored in the index.
pub struct BookmarkContent {
    /// Event_id of the current "version" of the
    /// content of this bookmark. Bookmarks may have
    /// different versions when `m.replace` relations
    /// exist on the bookmarked message.
    pub(super) event_id: OwnedEventId,
    /// Plain text content of the bookmarked event.
    /// The content of this field will be indexed.
    /// It may be None if the bookmarked event does
    /// not have suitable text fields to pass.
    pub(super) body: Option<String>,
    /// Timestamp when the bookmarked event has been
    /// sent.
    pub(super) date: MilliSecondsSinceUnixEpoch,
    /// Matrix UserId of the sender of this bookmarked
    /// event.
    pub(super) sender: OwnedUserId,
}

impl BookmarkContent {
    /// Create a new BookmarkContent
    pub fn new(
        event_id: OwnedEventId,
        body: Option<String>,
        date: MilliSecondsSinceUnixEpoch,
        sender: OwnedUserId,
    ) -> Self {
        Self { event_id, body, date, sender }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct BookmarkSchema {
    inner: Schema,
    /// The event id of the current version of the bookmarked
    /// message. (primary key).
    event_id_field: Field,
    /// The event id of the original version of the bookmarked message.
    /// Used by edits to refer to the event they edited (deletion key).
    original_event_id_field: Field,
    /// The event id of the bookmark "pointer" event.
    /// It is also used as a key.
    pointer_event_id_field: Field,
    body_field: Field,
    date_field: Field,
    sender_field: Field,
    room_id_field: Field,
    default_search_fields: Vec<Field>,
}

impl MatrixBookmarkIndexSchema for BookmarkSchema {
    fn new() -> Self {
        let mut schema = Schema::builder();
        let event_id_field = schema.add_text_field("event_id", STORED | STRING);
        let original_event_id_field = schema.add_text_field("original_event_id", STORED | STRING);
        let pointer_event_id_field = schema.add_text_field("pointer_event_id", STORED | STRING);
        let body_field = schema.add_text_field("body", STORED | TEXT);

        let date_options = DateOptions::from(STORED | INDEXED)
            .set_fast()
            .set_precision(DateTimePrecision::Seconds);

        let date_field = schema.add_date_field("date", date_options);
        let sender_field = schema.add_text_field("sender", STORED | STRING);
        let room_id_field = schema.add_text_field("room_id", STORED | STRING);

        let default_search_fields = vec![body_field];

        let schema = schema.build();

        Self {
            inner: schema,
            event_id_field,
            original_event_id_field,
            pointer_event_id_field,
            body_field,
            date_field,
            sender_field,
            room_id_field,
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

    fn pointer_event_id_key(&self) -> Field {
        self.pointer_event_id_field
    }

    fn body_key(&self) -> Field {
        self.body_field
    }

    fn date_key(&self) -> Field {
        self.date_field
    }

    fn room_id_key(&self) -> Field {
        self.room_id_field
    }

    fn sender_key(&self) -> Field {
        self.sender_field
    }

    fn get_field_name(&self, field: Field) -> &str {
        self.inner.get_field_name(field)
    }

    fn as_tantivy_schema(&self) -> Schema {
        self.inner.clone()
    }

    /// Given a [`BookmarkPointerInfo`] and a [`BookmarkContent`]
    /// return a [`TantivyDocument`].
    fn make_doc(
        &self,
        pointer_info: BookmarkPointerInfo,
        bookmark_content: BookmarkContent,
    ) -> Result<TantivyDocument, IndexError> {
        let document = doc!(
            self.body_field => bookmark_content.body.unwrap_or("".to_owned()),
            self.date_field =>
                DateTime::from_timestamp_millis(
                    bookmark_content.date.get().into()),
            self.sender_field => bookmark_content.sender.to_string(),
            self.event_id_field => bookmark_content.event_id.to_string(),
            self.original_event_id_field => pointer_info.original_event_id.to_string(),
            self.pointer_event_id_field => pointer_info.pointer_event_id.to_string(),
            self.room_id_field => pointer_info.room_id.to_string(),
        );

        Ok(document)
    }
}

impl TryFrom<Schema> for BookmarkSchema {
    type Error = IndexSchemaError;

    fn try_from(schema: Schema) -> Result<BookmarkSchema, Self::Error> {
        let event_id_field = schema.get_field("event_id")?;
        let original_event_id_field = schema.get_field("original_event_id")?;
        let pointer_event_id_field = schema.get_field("pointer_event_id")?;
        let body_field = schema.get_field("body")?;
        let date_field = schema.get_field("date")?;
        let sender_field = schema.get_field("sender")?;
        let room_id_field = schema.get_field("room_id")?;

        let default_search_fields = vec![body_field];

        Ok(Self {
            inner: schema,
            event_id_field,
            original_event_id_field,
            pointer_event_id_field,
            body_field,
            date_field,
            sender_field,
            room_id_field,
            default_search_fields,
        })
    }
}
