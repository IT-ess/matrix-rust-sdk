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
//! A module for managing a [`BookmarkIndex`]

pub mod builder;
mod schema;
mod writer;

use std::{collections::HashSet, fmt};

use ruma::{
    EventId, MilliSecondsSinceUnixEpoch, OwnedEventId, OwnedRoomId, OwnedUserId, RoomId, UInt,
    UserId, events::bookmark::OriginalSyncBookmarkEvent,
};
use tantivy::{
    Index, IndexReader, TantivyDocument, collector::TopDocs, directory::error::OpenDirectoryError,
    query::QueryParser, schema::Value,
};
use tracing::{debug, warn};

use crate::{
    OpStamp, TANTIVY_INDEX_MEMORY_BUDGET,
    bookmarks::{
        schema::{BookmarkSchema, MatrixBookmarkIndexSchema},
        writer::BookmarkIndexWriter,
    },
    error::{BookmarkIndexError, IndexError},
};

pub use crate::bookmarks::schema::BookmarkContent;

/// A struct to represent the operations on a [`BookmarkIndex`]
#[derive(Debug, Clone)]
pub enum BookmarkIndexOperation {
    /// Add this bookmark to the index.
    Add(OriginalSyncBookmarkEvent, BookmarkContent),
    /// Remove all documents in the index where
    /// `MatrixBookmarkIndexSchema::deletion_key()` matches this event id.
    Remove(OwnedEventId),
    /// Replace all documents in the index where
    /// `MatrixBookmarkIndexSchema::deletion_key()` matches this event id with
    /// the new event.
    Edit(OwnedEventId, BookmarkContent),
    /// Do nothing.
    Noop,
}

/// A struct that holds all data pertaining to the global bookmarks index.
pub struct BookmarkIndex {
    index: Index,
    schema: BookmarkSchema,
    query_parser: QueryParser,
    uncommitted_adds: HashSet<OwnedEventId>,
    uncommitted_removes: HashSet<OwnedEventId>,
}

impl fmt::Debug for BookmarkIndex {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BookmarkIndex").field("schema", &self.schema).finish()
    }
}

impl BookmarkIndex {
    pub(crate) fn new_with(index: Index, schema: BookmarkSchema) -> BookmarkIndex {
        let query_parser = QueryParser::for_index(&index, schema.default_search_fields());
        Self {
            index,
            schema,
            query_parser,
            uncommitted_adds: HashSet::new(),
            uncommitted_removes: HashSet::new(),
        }
    }

    /// Get a [`BookmarkIndexWriter`] for this index.
    fn get_writer(&self) -> Result<BookmarkIndexWriter, IndexError> {
        let writer = self.index.writer(TANTIVY_INDEX_MEMORY_BUDGET)?;
        Ok(BookmarkIndexWriter::new(writer, self.schema.clone()))
    }

    /// Get a [`IndexReader`] for this index.
    fn get_reader(&self) -> Result<IndexReader, IndexError> {
        Ok(self.index.reader_builder().try_into()?)
    }

    /// Commit added events to [`BookmarkIndex`]. The changes are not reflected in
    /// the search results until the serchers are reloaded.
    ///
    /// Use [`BookmarkIndex::commit_and_reload`] for this purpose.
    fn commit(&mut self, writer: &mut BookmarkIndexWriter) -> Result<OpStamp, IndexError> {
        let last_commit_opstamp = writer.commit()?; // TODO: This is blocking. Handle it.
        self.uncommitted_adds.clear();
        self.uncommitted_removes.clear();
        Ok(last_commit_opstamp)
    }

    /// Commit added events to [`BookmarkIndex`] and
    /// update searchers so that they reflect the state of the last
    /// `.commit()`.
    ///
    /// Every commit should be rapidly reflected on your `IndexReader` and you
    /// should not need to call `reload()` at all.
    ///
    /// This automatic reload can take 10s of milliseconds to kick in however,
    /// and in unit tests it can be nice to deterministically force the
    /// reload of searchers.
    fn commit_and_reload(
        &mut self,
        writer: &mut BookmarkIndexWriter,
    ) -> Result<OpStamp, IndexError> {
        debug!(
            "BookmarkIndex: committing and reloading: uncommitted: {:?}, {:?}",
            self.uncommitted_adds, self.uncommitted_removes
        );
        let last_commit_opstamp = self.commit(writer)?;
        self.get_reader()?.reload()?;
        Ok(last_commit_opstamp)
    }

    /// Search the [`BookmarkIndex`] for some query. Returns a list of
    /// results with a maximum given length. If `pagination_offset` is
    /// set then the results will start there, i.e.
    ///
    /// if `max_number_of_results = 3` and `pagination_offset = 10`
    /// (and there are a surplus of results)
    /// then this will return results `11, 12, 13`
    pub fn search(
        &self,
        query: &str,
        max_number_of_results: usize,
        pagination_offset: Option<usize>,
    ) -> Result<Vec<IndexedBookmark>, IndexError> {
        let query = self.query_parser.parse_query(query)?;
        let searcher = self.get_reader()?.searcher();

        let offset = pagination_offset.unwrap_or(0);

        let results = searcher.search(
            &query,
            &TopDocs::with_limit(max_number_of_results).and_offset(offset).order_by_score(),
        )?;
        let mut ret: Vec<IndexedBookmark> = Vec::new();

        for (score, doc_address) in results {
            let retrieved_doc: TantivyDocument = searcher.doc(doc_address)?;

            // Helper closure to extract string data out of Tantivy's format safely
            let extract_str = |field| retrieved_doc.get_first(field).and_then(|v| v.as_str());

            let event_id = extract_str(self.schema.primary_key())
                .and_then(|str| EventId::parse(str).ok())
                .ok_or(IndexError::IdParsing)?;

            let original_event_id = extract_str(self.schema.deletion_key())
                .and_then(|str| EventId::parse(str).ok())
                .ok_or(IndexError::IdParsing)?;

            let pointer_event_id = extract_str(self.schema.pointer_event_id_key())
                .and_then(|str| EventId::parse(str).ok())
                .ok_or(IndexError::IdParsing)?;

            let sender = extract_str(self.schema.sender_key())
                .and_then(|str| UserId::parse(str).ok())
                .ok_or(IndexError::IdParsing)?;

            let room_id = extract_str(self.schema.room_id_key())
                .and_then(|str| RoomId::parse(str).ok())
                .ok_or(IndexError::IdParsing)?;

            let date_value = retrieved_doc
                .get_first(self.schema.date_key())
                .and_then(|v| v.as_datetime())
                .ok_or(IndexError::IdParsing)?;

            let date = MilliSecondsSinceUnixEpoch(UInt::new_saturating(
                date_value.into_timestamp_millis() as u64,
            ));

            ret.push(IndexedBookmark {
                body: extract_str(self.schema.body_key()).unwrap_or_default().to_owned(),
                event_id,
                original_event_id,
                pointer_event_id,
                original_server_ts: date,
                sender,
                room_id,
                score,
            });
        }

        Ok(ret)
    }

    fn get_events_to_be_removed(
        &self,
        event_id: &EventId,
    ) -> Result<Vec<IndexedBookmark>, IndexError> {
        self.search(
            format!("{}:\"{event_id}\"", self.schema.get_field_name(self.schema.deletion_key()))
                .as_str(),
            10000,
            None,
        )
    }

    fn add(
        &mut self,
        writer: &mut BookmarkIndexWriter,
        pointer_info: BookmarkPointerInfo,
        bookmark_content: BookmarkContent,
    ) -> Result<(), IndexError> {
        let current_version_event_id = bookmark_content.event_id.clone();
        if !self.contains(&current_version_event_id) {
            writer.add(self.schema.make_doc(pointer_info, bookmark_content)?)?;
        }
        self.uncommitted_removes.remove(&current_version_event_id);
        self.uncommitted_adds.insert(current_version_event_id);
        Ok(())
    }

    fn remove(
        &mut self,
        writer: &mut BookmarkIndexWriter,
        original_event_id: OwnedEventId,
    ) -> Result<BookmarkPointerInfo, IndexError> {
        let events = self.get_events_to_be_removed(&original_event_id)?;

        writer.remove(&original_event_id);

        // When we edit an event, we remove the previous one(s) and then recreate
        // it. We need to pass some info from the previously saved bookmark for this
        // to work.
        let Some(pointer_info) = events.first().map(|bookmark| BookmarkPointerInfo {
            original_event_id,
            room_id: bookmark.room_id.clone(),
            pointer_event_id: bookmark.pointer_event_id.clone(),
        }) else {
            return Err(IndexError::BookmarkIndexError(BookmarkIndexError::MissingData));
        };

        for event in events.into_iter() {
            self.uncommitted_adds.remove(&event.event_id);
            self.uncommitted_removes.insert(event.event_id);
        }

        Ok(pointer_info)
    }

    fn execute_impl(
        &mut self,
        writer: &mut BookmarkIndexWriter,
        operation: &BookmarkIndexOperation,
    ) -> Result<(), IndexError> {
        debug!("INDEX: executing {operation:?}");
        match operation.clone() {
            BookmarkIndexOperation::Add(pointer_event, bookmark_content) => {
                self.add(writer, pointer_event.into(), bookmark_content)?;
            }
            BookmarkIndexOperation::Remove(event_id) => {
                self.remove(writer, event_id)?;
            }
            BookmarkIndexOperation::Edit(original_event_id, bookmark_content) => {
                let pointer_info = self.remove(writer, original_event_id)?;
                self.add(writer, pointer_info, bookmark_content)?;
            }
            BookmarkIndexOperation::Noop => {}
        }
        Ok(())
    }

    /// Execute [`BookmarkIndexOperation`] with retry
    fn execute_with_retry(
        &mut self,
        writer: &mut BookmarkIndexWriter,
        operation: &BookmarkIndexOperation,
        retries: usize,
    ) -> Result<(), IndexError> {
        let mut num_tries = 0;

        while let Err(err) = self.execute_impl(writer, operation) {
            if num_tries == retries {
                return Err(err);
            }
            match err {
                // Retry
                IndexError::TantivyError(_)
                | IndexError::IndexSchemaError(_)
                | IndexError::IndexWriteError(_)
                | IndexError::IO(_) => {
                    num_tries += 1;
                }
                IndexError::OpenDirectoryError(ref e) => match e {
                    // Retry
                    OpenDirectoryError::IoError { io_error: _, directory_path: _ } => {
                        num_tries += 1;
                    }
                    // Bubble
                    OpenDirectoryError::DoesNotExist(_)
                    | OpenDirectoryError::FailedToCreateTempDir(_)
                    | OpenDirectoryError::NotADirectory(_) => return Err(err),
                },
                // Bubble
                IndexError::QueryParserError(_)
                | IndexError::BookmarkIndexError(_)
                | IndexError::IdParsing => {
                    return Err(err);
                }
                // Ignore
                IndexError::CannotIndexRedactedMessage
                | IndexError::EmptyMessage
                | IndexError::MessageTypeNotSupported => break,
            }
            debug!("Failed to execute operation in room index (try {num_tries}): {err}");
        }
        Ok(())
    }

    /// Execute [`BookmarkIndexOperation`]
    ///
    /// If an error occurs, retry 5 times if possible.
    ///
    /// This which will add/remove/edit an event in the index based on the
    /// operation.
    ///
    /// Prefer [`BookmarkIndex::bulk_execute`] for multiple operations.
    pub fn execute(&mut self, operation: BookmarkIndexOperation) -> Result<(), IndexError> {
        let mut writer = self.get_writer()?;
        self.execute_with_retry(&mut writer, &operation, 5)?;
        self.commit_and_reload(&mut writer)?;
        Ok(())
    }

    /// Bulk execute [`BookmarkIndexOperation`]s
    ///
    /// If an error occurs in the batch it retries 5 times if possible.
    ///
    /// This which will add/remove/edit an events in the index based on the
    /// operations.
    pub fn bulk_execute(
        &mut self,
        operations: Vec<BookmarkIndexOperation>,
    ) -> Result<(), IndexError> {
        let mut writer = self.get_writer()?;
        let mut operations = operations.into_iter();
        let mut next_operation = operations.next();

        while let Some(ref operation) = next_operation {
            self.execute_with_retry(&mut writer, operation, 5)?;
            next_operation = operations.next();
        }

        self.commit_and_reload(&mut writer)?;

        Ok(())
    }

    fn contains(&self, event_id: &EventId) -> bool {
        let search_result = self.search(
            format!("{}:\"{event_id}\"", self.schema.get_field_name(self.schema.primary_key()))
                .as_str(),
            1,
            None,
        );
        match search_result {
            Ok(results) => {
                !self.uncommitted_removes.contains(event_id)
                    && (!results.is_empty() || self.uncommitted_adds.contains(event_id))
            }
            Err(err) => {
                warn!("Failed to check if event has been indexed, assuming it has: {err}");
                true
            }
        }
    }

    /// Check the presence of a record with its deletion key (the original version of the bookmarked
    /// message)
    pub fn contains_bookmark(&self, original_event_id: &EventId) -> bool {
        let search_result = self.search(
            format!(
                "{}:\"{original_event_id}\"",
                self.schema.get_field_name(self.schema.deletion_key())
            )
            .as_str(),
            1,
            None,
        );
        match search_result {
            Ok(results) => !results.is_empty(),
            Err(err) => {
                warn!("Failed to check if event has been indexed, assuming it wasn't: {err}");
                false
            }
        }
    }
}

/// Necessary information to identify a unique bookmark.
#[derive(Debug)]
pub struct BookmarkPointerInfo {
    /// The event_id of the root event of the bookmarked
    /// event
    pub(super) original_event_id: OwnedEventId,
    /// The room_id of the bookmarked event
    pub(super) room_id: OwnedRoomId,
    /// The event_id of the [`OriginalSyncBookmarkEvent`]
    pub(super) pointer_event_id: OwnedEventId,
}

impl From<OriginalSyncBookmarkEvent> for BookmarkPointerInfo {
    fn from(value: OriginalSyncBookmarkEvent) -> Self {
        Self {
            original_event_id: value.content.pointer.event_id,
            room_id: value.content.pointer.room_id,
            pointer_event_id: value.event_id,
        }
    }
}

/// Representation of a bookmark as it is stored
/// in the index.
#[derive(Debug, Clone)]
pub struct IndexedBookmark {
    /// Event id of the current "version" of the bookmarked
    /// message (latest event of the `m.replace` relation chain)
    pub event_id: OwnedEventId,
    /// "Root" event id of the bookmarked message (first event of
    /// the `m.replace` relation chain)
    pub original_event_id: OwnedEventId,
    /// Event id of the `m.bookmark` event that points to the
    /// bookmarked event and triggered its indexation.
    pub pointer_event_id: OwnedEventId,
    /// Body of the bookmarked message. Maybe an empty string if
    /// the event does not have a string representation.
    pub body: String,
    /// When the bookmarked event has been sent
    pub original_server_ts: MilliSecondsSinceUnixEpoch,
    /// Sender of the bookmarked event
    pub sender: OwnedUserId,
    /// Room in which the bookmarked event lives
    pub room_id: OwnedRoomId,
    /// Search score
    pub score: f32,
}

// #[cfg(test)]
// mod tests {
//     use std::{collections::HashSet, error::Error};

//     use matrix_sdk_test::event_factory::EventFactory;
//     use ruma::{
//         EventId, event_id,
//         events::{
//             AnySyncMessageLikeEvent,
//             room::message::{OriginalSyncRoomMessageEvent, RoomMessageEventContentWithoutRelation},
//         },
//         room_id, user_id,
//     };

//     use crate::{
//         bookmarks::{BookmarkIndex, BookmarkIndexOperation},
//         error::IndexError,
//     };

//     /// Helper function to add a bookmark to the index
//     fn index_message(
//         index: &mut BookmarkIndex,
//         event: AnySyncMessageLikeEvent,
//     ) -> Result<(), IndexError> {
//         if let AnySyncMessageLikeEvent::RoomMessage(ev) = event
//             && let Some(ev) = ev.as_original()
//             && ev.content.relates_to.is_none()
//         {
//             return index.execute(BookmarkIndexOperation::Add(ev.clone()));
//         }
//         panic!("Event was not a relationless OriginalSyncRoomMessageEvent.")
//     }

//     /// Helper function to remove events to the index
//     fn index_remove(index: &mut BookmarkIndex, event_id: &EventId) -> Result<(), IndexError> {
//         index.execute(BookmarkIndexOperation::Remove(event_id.to_owned()))
//     }

//     /// Helper function to edit events in index
//     ///
//     /// Edit event with `event_id` into new [`OriginalSyncRoomMessageEvent`]
//     fn index_edit(
//         index: &mut BookmarkIndex,
//         event_id: &EventId,
//         new: OriginalSyncRoomMessageEvent,
//     ) -> Result<(), IndexError> {
//         index.execute(BookmarkIndexOperation::Edit(event_id.to_owned(), new))
//     }

//     #[test]
//     fn test_add_event() {
//         let room_id = room_id!("!room_id:localhost");
//         let mut index = BookmarkIndexBuilder::new_in_memory(room_id).build();

//         let event = EventFactory::new()
//             .text_msg("event message")
//             .event_id(event_id!("$event_id:localhost"))
//             .room(room_id)
//             .sender(user_id!("@user_id:localhost"))
//             .into_any_sync_message_like_event();

//         index_message(&mut index, event).expect("failed to add event: {res:?}");
//     }

//     #[test]
//     fn test_search_populated_index() -> Result<(), Box<dyn Error>> {
//         let room_id = room_id!("!room_id:localhost");
//         let mut index = BookmarkIndexBuilder::new_in_memory(room_id).build();

//         let event_id_1 = event_id!("$event_id_1:localhost");
//         let event_id_2 = event_id!("$event_id_2:localhost");
//         let event_id_3 = event_id!("$event_id_3:localhost");
//         let user_id = user_id!("@user_id:localhost");
//         let f = EventFactory::new().room(room_id).sender(user_id);

//         index_message(
//             &mut index,
//             f.text_msg("This is a sentence")
//                 .event_id(event_id_1)
//                 .into_any_sync_message_like_event(),
//         )?;

//         index_message(
//             &mut index,
//             f.text_msg("All new words").event_id(event_id_2).into_any_sync_message_like_event(),
//         )?;

//         index_message(
//             &mut index,
//             f.text_msg("A similar sentence")
//                 .event_id(event_id_3)
//                 .into_any_sync_message_like_event(),
//         )?;

//         let result = index.search("sentence", 10, None).expect("search failed with: {result:?}");
//         let result: HashSet<_> = result.iter().collect();

//         let true_value = [event_id_1.to_owned(), event_id_3.to_owned()];
//         let true_value: HashSet<_> = true_value.iter().collect();

//         assert_eq!(result, true_value, "search result not correct: {result:?}");

//         Ok(())
//     }

//     #[test]
//     fn test_search_empty_index() -> Result<(), Box<dyn Error>> {
//         let room_id = room_id!("!room_id:localhost");
//         let index = BookmarkIndexBuilder::new_in_memory(room_id).build();

//         let result = index.search("sentence", 10, None).expect("search failed with: {result:?}");

//         assert!(result.is_empty(), "search result not empty: {result:?}");

//         Ok(())
//     }

//     #[test]
//     fn test_index_contains_false() {
//         let room_id = room_id!("!room_id:localhost");
//         let index = BookmarkIndexBuilder::new_in_memory(room_id).build();

//         let event_id = event_id!("$event_id:localhost");

//         assert!(!index.contains(event_id), "Index should not contain event");
//     }

//     #[test]
//     fn test_index_contains_true() -> Result<(), Box<dyn Error>> {
//         let room_id = room_id!("!room_id:localhost");
//         let mut index = BookmarkIndexBuilder::new_in_memory(room_id).build();

//         let event_id = event_id!("$event_id:localhost");
//         let event = EventFactory::new()
//             .text_msg("This is a sentence")
//             .event_id(event_id)
//             .room(room_id)
//             .sender(user_id!("@user_id:localhost"))
//             .into_any_sync_message_like_event();

//         index_message(&mut index, event)?;

//         assert!(index.contains(event_id), "Index should contain event");

//         Ok(())
//     }

//     #[test]
//     fn test_index_add_idempotency() -> Result<(), Box<dyn Error>> {
//         let room_id = room_id!("!room_id:localhost");
//         let mut index = BookmarkIndexBuilder::new_in_memory(room_id).build();

//         let event_id = event_id!("$event_id:localhost");
//         let event = EventFactory::new()
//             .text_msg("This is a sentence")
//             .event_id(event_id)
//             .room(room_id)
//             .sender(user_id!("@user_id:localhost"))
//             .into_any_sync_message_like_event();

//         index_message(&mut index, event.clone())?;

//         assert!(index.contains(event_id), "Index should contain event");

//         // indexing again should do nothing
//         index_message(&mut index, event)?;

//         assert!(index.contains(event_id), "Index should still contain event");

//         let result = index.search("sentence", 10, None).expect("search failed with: {result:?}");

//         assert_eq!(result.len(), 1, "Index should have ignored second indexing");

//         Ok(())
//     }

//     #[test]
//     fn test_remove_event() -> Result<(), Box<dyn Error>> {
//         let room_id = room_id!("!room_id:localhost");
//         let mut index = BookmarkIndexBuilder::new_in_memory(room_id).build();

//         let event_id = event_id!("$event_id:localhost");
//         let user_id = user_id!("@user_id:localhost");
//         let f = EventFactory::new().room(room_id).sender(user_id);

//         let event =
//             f.text_msg("This is a sentence").event_id(event_id).into_any_sync_message_like_event();

//         index_message(&mut index, event)?;

//         assert!(index.contains(event_id), "Index should contain event");

//         index_remove(&mut index, event_id)?;

//         assert!(!index.contains(event_id), "Index should not contain event");

//         Ok(())
//     }

//     #[test]
//     fn test_edit_removes_old_and_adds_new_event() -> Result<(), Box<dyn Error>> {
//         let room_id = room_id!("!room_id:localhost");
//         let mut index = BookmarkIndexBuilder::new_in_memory(room_id).build();

//         let old_event_id = event_id!("$old_event_id:localhost");
//         let user_id = user_id!("@user_id:localhost");
//         let f = EventFactory::new().room(room_id).sender(user_id);

//         let old_event = f
//             .text_msg("This is a sentence")
//             .event_id(old_event_id)
//             .into_any_sync_message_like_event();

//         index_message(&mut index, old_event)?;

//         assert!(index.contains(old_event_id), "Index should contain event");

//         let new_event_id = event_id!("$new_event_id:localhost");
//         let edit = f
//             .text_msg("This is a brand new sentence!")
//             .edit(
//                 old_event_id,
//                 RoomMessageEventContentWithoutRelation::text_plain("This is a brand new sentence!"),
//             )
//             .event_id(new_event_id)
//             .into_original_sync_room_message_event();

//         index_edit(&mut index, old_event_id, edit)?;

//         assert!(!index.contains(old_event_id), "Index should not contain old event");
//         assert!(index.contains(new_event_id), "Index should contain edited event");

//         Ok(())
//     }
// }
