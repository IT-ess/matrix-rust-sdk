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
    EventId, OwnedEventId, OwnedRoomId, RoomId,
    events::{bookmark::OriginalSyncBookmarkEvent, room::message::Relation},
};
use tantivy::{
    Index, IndexReader, TantivyDocument, Term,
    collector::TopDocs,
    directory::error::OpenDirectoryError,
    query::{AllQuery, BooleanQuery, Occur, Query, QueryParser, TermQuery},
    schema::{Field, IndexRecordOption, Value},
};
use tracing::{debug, info, warn};

use crate::{
    OpStamp, TANTIVY_INDEX_MEMORY_BUDGET,
    bookmarks::{
        schema::{BookmarkSchema, MatrixBookmarkIndexSchema},
        writer::BookmarkIndexWriter,
    },
    error::IndexError,
    index::RoomIndexOperation,
};

pub use crate::bookmarks::schema::IndexedBookmarkContent;

/// A struct to represent the operations on a [`BookmarkIndex`]
#[derive(Debug, Clone)]
pub enum BookmarkIndexOperation {
    /// Add this bookmark to the index.
    Add(Box<OriginalSyncBookmarkEvent>, IndexedBookmarkContent),
    /// Remove all documents in the index where
    /// `MatrixBookmarkIndexSchema::deletion_key()` matches this event id.
    Remove(OwnedEventId),
    /// Remove all documents in the index where
    /// `MatrixBookmarkIndexSchema::target_event_id_key()` matches this event id.
    RemoveWithTargetEventId(OwnedEventId),
    /// Replace all documents in the index where
    /// `MatrixBookmarkIndexSchema::deletion_key()` matches this original_event id
    /// of the BookmarkPointerInfo
    Edit(BookmarkPointerInfo, IndexedBookmarkContent),
    /// Replace all documents in the index where
    /// `MatrixBookmarkIndexSchema::target_event_id_key()` matches this event_id
    EditWithTargetEventId(OwnedEventId, IndexedBookmarkContent),
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
    /// If a room_id is provided, only the bookmarks from this room will
    /// be returned.
    ///
    /// if `max_number_of_results = 3` and `pagination_offset = 10`
    /// (and there are a surplus of results)
    /// then this will return results `11, 12, 13`
    pub fn search(
        &self,
        query: &str,
        max_number_of_results: usize,
        pagination_offset: Option<usize>,
        room_id_filter: Option<&RoomId>,
    ) -> Result<Vec<IndexedBookmark>, IndexError> {
        // A `*` (or empty) query matches every bookmark; otherwise the body must
        // match the query. Note that the body query must be `Must` (required), not
        // `Should`: a `Should` clause is optional as soon as another clause (such as
        // the room filter below) is `Must`, which would make every bookmark in the
        // room match regardless of the query.
        let base_query: Option<Box<dyn Query>> = if query == "*" || query.is_empty() {
            None
        } else {
            Some(self.query_parser.parse_query(query)?)
        };

        let room_filter_query: Option<Box<dyn Query>> = match room_id_filter {
            Some(room_id) => {
                let room_term =
                    Term::from_field_text(self.schema.target_room_id_key(), room_id.as_str());
                Some(Box::new(TermQuery::new(room_term, IndexRecordOption::Basic)))
            }
            None => None,
        };

        let full_query: Box<dyn Query> = match (base_query, room_filter_query) {
            (Some(base), Some(room_filter)) => {
                Box::new(BooleanQuery::new(vec![(Occur::Must, base), (Occur::Must, room_filter)]))
            }
            (Some(base), None) => base,
            (None, Some(room_filter)) => room_filter,
            (None, None) => Box::new(AllQuery),
        };

        self.run_query(full_query.as_ref(), max_number_of_results, pagination_offset)
    }

    /// Run a tantivy [`Query`] and reconstruct the matching
    /// [`IndexedBookmark`]s.
    fn run_query(
        &self,
        query: &dyn Query,
        max_number_of_results: usize,
        pagination_offset: Option<usize>,
    ) -> Result<Vec<IndexedBookmark>, IndexError> {
        let searcher = self.get_reader()?.searcher();

        let offset = pagination_offset.unwrap_or(0);

        let results = searcher.search(
            query,
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

            let target_event_id = extract_str(self.schema.target_event_id_key())
                .and_then(|str| EventId::parse(str).ok())
                .ok_or(IndexError::IdParsing)?;

            let target_room_id = extract_str(self.schema.target_room_id_key())
                .and_then(|str| RoomId::parse(str).ok())
                .ok_or(IndexError::IdParsing)?;

            ret.push(IndexedBookmark {
                event_id,
                original_event_id,
                target_event_id,
                target_room_id,
                score,
            });
        }

        Ok(ret)
    }

    /// Find all indexed bookmarks for which the given text `field` exactly
    /// matches `event_id`.
    fn find_by_event_id_field(
        &self,
        field: Field,
        event_id: &EventId,
        max_number_of_results: usize,
    ) -> Result<Vec<IndexedBookmark>, IndexError> {
        let term = Term::from_field_text(field, event_id.as_str());
        let query = TermQuery::new(term, IndexRecordOption::Basic);
        self.run_query(&query, max_number_of_results, None)
    }

    fn get_events_to_be_removed(
        &self,
        event_id: &EventId,
    ) -> Result<Vec<IndexedBookmark>, IndexError> {
        self.find_by_event_id_field(self.schema.deletion_key(), event_id, 10000)
    }

    fn add(
        &mut self,
        writer: &mut BookmarkIndexWriter,
        pointer_info: BookmarkPointerInfo,
        bookmark_content: IndexedBookmarkContent,
    ) -> Result<(), IndexError> {
        let event_id = pointer_info.event_id.clone();
        if !self.contains(&event_id) {
            writer.add(self.schema.make_doc(pointer_info, bookmark_content)?)?;
        }
        self.uncommitted_removes.remove(&event_id);
        self.uncommitted_adds.insert(event_id);
        Ok(())
    }

    fn remove(
        &mut self,
        writer: &mut BookmarkIndexWriter,
        original_event_id: &EventId,
    ) -> Result<(), IndexError> {
        let events = self.get_events_to_be_removed(original_event_id)?;

        writer.remove(original_event_id);

        for event in events.into_iter() {
            self.uncommitted_adds.remove(&event.event_id);
            self.uncommitted_removes.insert(event.event_id);
        }

        Ok(())
    }

    fn execute_impl(
        &mut self,
        writer: &mut BookmarkIndexWriter,
        operation: &BookmarkIndexOperation,
    ) -> Result<(), IndexError> {
        match operation.clone() {
            BookmarkIndexOperation::Add(pointer_event, bookmark_content) => {
                self.add(writer, (*pointer_event).into(), bookmark_content)?;
            }
            BookmarkIndexOperation::Remove(event_id) => {
                self.remove(writer, &event_id)?;
            }
            BookmarkIndexOperation::RemoveWithTargetEventId(target_event_id) => {
                if let Some(pointer_info) =
                    self.get_pointer_info_from_target_event(&target_event_id)
                {
                    self.remove(writer, &pointer_info.original_event_id)?;
                } else {
                    info!(
                        "There was no bookmark to remove for given target_event_id {target_event_id}."
                    )
                }
            }
            BookmarkIndexOperation::Edit(pointer_info, bookmark_content) => {
                self.remove(writer, &pointer_info.original_event_id)?;
                self.add(writer, pointer_info, bookmark_content)?;
            }
            BookmarkIndexOperation::EditWithTargetEventId(target_event_id, bookmark_content) => {
                if let Some(pointer_info) =
                    self.get_pointer_info_from_target_event(&target_event_id)
                {
                    self.remove(writer, &pointer_info.original_event_id)?;
                    self.add(writer, pointer_info, bookmark_content)?;
                } else {
                    info!(
                        "There was no bookmark to edit for given target_event_id {target_event_id}."
                    )
                }
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
                IndexError::QueryParserError(_) | IndexError::IdParsing => {
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
        let search_result = self.find_by_event_id_field(self.schema.primary_key(), event_id, 1);
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

    #[cfg(test)]
    /// Check the presence of a record with its target_event_id key
    fn contains_bookmark(&self, target_event_id: &EventId) -> bool {
        match self.find_by_event_id_field(self.schema.target_event_id_key(), target_event_id, 100) {
            Ok(results) => !results.is_empty(),
            Err(err) => {
                warn!("Failed to check if event has been indexed, assuming it wasn't: {err}");
                false
            }
        }
    }

    /// Check from a target_event_id if there is a bookmark, and return
    /// its [`BookmarkPointerInfo`] if its the case.
    pub fn get_pointer_info_from_target_event(
        &self,
        target_event_id: &EventId,
    ) -> Option<BookmarkPointerInfo> {
        match self.find_by_event_id_field(self.schema.target_event_id_key(), target_event_id, 100) {
            Ok(results) => results.into_iter().next().map(Into::into),
            Err(err) => {
                warn!("Failed to check if event has been indexed, assuming it wasn't: {err}");
                None
            }
        }
    }
}

/// Necessary information to identify a unique bookmark.
#[derive(Debug, Clone, PartialEq)]
pub struct BookmarkPointerInfo {
    /// The event_id of the [`OriginalSyncBookmarkEvent`]
    pub(super) event_id: OwnedEventId,
    /// The event_id of the [`OriginalSyncBookmarkEvent`] or
    /// the event it replaces if its the case.
    pub(super) original_event_id: OwnedEventId,
    /// The room_id of the bookmarked event.
    pub(super) target_room_id: OwnedRoomId,
    /// The event_id of the bookmarked event.
    pub(super) target_event_id: OwnedEventId,
}

impl From<OriginalSyncBookmarkEvent> for BookmarkPointerInfo {
    fn from(value: OriginalSyncBookmarkEvent) -> Self {
        if let Some(Relation::Replacement(replacement_data)) = value.content.relates_to {
            Self {
                event_id: value.event_id,
                original_event_id: replacement_data.event_id,
                target_event_id: replacement_data.new_content.pointer.event_id,
                target_room_id: replacement_data.new_content.pointer.room_id,
            }
        } else {
            Self {
                event_id: value.event_id.clone(),
                original_event_id: value.event_id,
                target_event_id: value.content.pointer.event_id,
                target_room_id: value.content.pointer.room_id,
            }
        }
    }
}

impl BookmarkPointerInfo {
    /// Get the bookmark_id of this bookmark (corresponds to original_event_id)
    pub fn bookmark_id(self) -> OwnedEventId {
        self.original_event_id
    }
}

/// Representation of the stored fields in the index
#[derive(Debug, Clone)]
pub struct IndexedBookmark {
    /// Event id of the current "version" of the bookmark event
    pub event_id: OwnedEventId,
    /// "Root" event id of the bookmark event (first event of
    /// the `m.replace` relation chain)
    pub original_event_id: OwnedEventId,
    /// Event id of the event targeted by this bookmark
    pub target_event_id: OwnedEventId,
    /// Room in which the bookmarked event lives
    pub target_room_id: OwnedRoomId,
    /// Search score
    pub score: f32,
}

impl From<IndexedBookmark> for BookmarkPointerInfo {
    fn from(value: IndexedBookmark) -> Self {
        Self {
            event_id: value.event_id,
            original_event_id: value.original_event_id,
            target_room_id: value.target_room_id,
            target_event_id: value.target_event_id,
        }
    }
}

/// Returns a [`BookmarkIndexOperation`] if an event has been edited
/// or removed in a room. A check will be done by the bookmark index
/// and eventually apply operations to the index.
pub fn derive_bookmark_operation_from_regular_index_operation(
    operation: &RoomIndexOperation,
) -> Option<BookmarkIndexOperation> {
    match operation {
        RoomIndexOperation::Edit(event_id, new_content) => {
            Some(BookmarkIndexOperation::EditWithTargetEventId(
                event_id.clone(),
                new_content.to_owned().into(),
            ))
        }
        RoomIndexOperation::Remove(event_id) => {
            Some(BookmarkIndexOperation::RemoveWithTargetEventId(event_id.clone()))
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use ruma::{
        MilliSecondsSinceUnixEpoch, event_id, owned_event_id, owned_room_id, owned_user_id, uint,
    };

    use super::{BookmarkIndex, BookmarkPointerInfo, IndexedBookmarkContent};
    use crate::bookmarks::builder::BookmarkIndexBuilder;

    /// Index a bookmark with the given (root) event id and body text, in a
    /// fixed default room.
    ///
    /// All the other fields are filled with throwaway-but-valid values so the
    /// tests can focus on body relevance/scoring.
    fn index_bookmark(index: &mut BookmarkIndex, event_id: &str, body: &str) {
        index_bookmark_in_room(index, event_id, body, "!room:example.org");
    }

    /// Index a bookmark with the given (root) event id, body text and room id.
    fn index_bookmark_in_room(
        index: &mut BookmarkIndex,
        event_id: &str,
        body: &str,
        room_id: &str,
    ) {
        let original_event_id = ruma::EventId::parse(event_id).unwrap();
        let pointer_event_id =
            ruma::EventId::parse(format!("$pointer-for-{}:example.org", &event_id[1..2])).unwrap();
        let room_id = ruma::RoomId::parse(room_id).unwrap();

        let pointer_info = BookmarkPointerInfo {
            event_id: original_event_id.clone(),
            original_event_id,
            target_room_id: room_id,
            target_event_id: pointer_event_id,
        };
        let content = IndexedBookmarkContent::new(
            body.to_owned(),
            MilliSecondsSinceUnixEpoch(uint!(0)),
            owned_user_id!("@alice:example.org"),
        );

        let mut writer = index.get_writer().unwrap();
        index.add(&mut writer, pointer_info, content).unwrap();
        index.commit_and_reload(&mut writer).unwrap();
    }

    /// Searching only returns bookmarks whose body matches the query, and the
    /// returned scores are strictly positive.
    #[test]
    fn test_search_body_only_returns_matching_bookmarks() {
        let mut index = BookmarkIndexBuilder::new_in_memory().build();

        let matching = owned_event_id!("$1-match:example.org");
        let other = owned_event_id!("$2-other:example.org");

        index_bookmark(&mut index, matching.as_str(), "the quick brown fox");
        index_bookmark(&mut index, other.as_str(), "completely unrelated content");

        let results = index.search("fox", 10, None, None).unwrap();

        assert_eq!(results.len(), 1, "only the matching bookmark should be returned");
        assert_eq!(results[0].original_event_id, matching);
        assert!(results[0].score > 0.0, "a matching bookmark must have a positive score");
    }

    /// A bookmark whose body mentions the query term more often is more relevant
    /// and must be ranked (scored) higher than one that mentions it only once.
    #[test]
    fn test_search_body_term_frequency_affects_score_ordering() {
        let mut index = BookmarkIndexBuilder::new_in_memory().build();

        let many = owned_event_id!("$1-many:example.org");
        let few = owned_event_id!("$2-few:example.org");

        index_bookmark(&mut index, many.as_str(), "matrix matrix matrix matrix matrix");
        index_bookmark(&mut index, few.as_str(), "matrix is a protocol for messaging");

        let results = index.search("matrix", 10, None, None).unwrap();

        assert_eq!(results.len(), 2, "both bookmarks mention the query term");

        // Results are ordered by descending score.
        assert_eq!(results[0].original_event_id, many);
        assert_eq!(results[1].original_event_id, few);
        assert!(
            results[0].score > results[1].score,
            "the body matching the term more often must score higher: {} vs {}",
            results[0].score,
            results[1].score,
        );
    }

    /// A bookmark matching several of the query terms must score higher than one
    /// matching only a single term.
    #[test]
    fn test_search_body_more_matching_terms_scores_higher() {
        let mut index = BookmarkIndexBuilder::new_in_memory().build();

        let full = owned_event_id!("$1-full:example.org");
        let partial = owned_event_id!("$2-part:example.org");

        index_bookmark(&mut index, full.as_str(), "quick brown fox");
        index_bookmark(&mut index, partial.as_str(), "quick green turtle");

        // An OR query: both bookmarks match "quick", but only one matches
        // "brown" and "fox" as well.
        let results = index.search("quick brown fox", 10, None, None).unwrap();

        assert_eq!(results.len(), 2);
        assert_eq!(results[0].original_event_id, full);
        assert_eq!(results[1].original_event_id, partial);
        assert!(
            results[0].score > results[1].score,
            "matching more query terms must score higher: {} vs {}",
            results[0].score,
            results[1].score,
        );
    }

    /// A rarer term carries more weight (higher IDF) than a term present in
    /// every document, so the bookmark matching the rare term ranks first.
    #[test]
    fn test_search_body_rare_term_scores_higher() {
        let mut index = BookmarkIndexBuilder::new_in_memory().build();

        let rare = owned_event_id!("$1-rare:example.org");
        let common_a = owned_event_id!("$2-coma:example.org");
        let common_b = owned_event_id!("$3-comb:example.org");

        // "common" appears in every document, "unicorn" only in one.
        index_bookmark(&mut index, rare.as_str(), "common unicorn");
        index_bookmark(&mut index, common_a.as_str(), "common ordinary thing");
        index_bookmark(&mut index, common_b.as_str(), "common everyday item");

        let results = index.search("common unicorn", 10, None, None).unwrap();

        println!("{results:?}");
        assert_eq!(results.len(), 3);
        assert_eq!(
            results[0].original_event_id, rare,
            "the bookmark matching the rare term should rank first",
        );
        assert!(results[0].score > results[1].score);
    }

    /// Searching for a term that no bookmark body contains returns nothing.
    #[test]
    fn test_search_body_no_match_returns_empty() {
        let mut index = BookmarkIndexBuilder::new_in_memory().build();

        index_bookmark(&mut index, "$1-a:example.org", "hello world");
        index_bookmark(&mut index, "$2-b:example.org", "goodbye world");

        let results = index.search("nonexistent", 10, None, None).unwrap();

        assert!(results.is_empty(), "no body matches the query: {results:?}");
    }

    /// A room_id filter restricts the results to bookmarks living in that room,
    /// even when bookmarks in other rooms match the query just as well.
    #[test]
    fn test_search_body_room_filter_excludes_other_rooms() {
        let mut index = BookmarkIndexBuilder::new_in_memory().build();

        let room_a = ruma::room_id!("!room-a:example.org");
        let room_b = ruma::room_id!("!room-b:example.org");

        let in_a = owned_event_id!("$1-a:example.org");
        let in_b = owned_event_id!("$2-b:example.org");

        index_bookmark_in_room(&mut index, in_a.as_str(), "shared matrix term", room_a.as_str());
        index_bookmark_in_room(&mut index, in_b.as_str(), "shared matrix term", room_b.as_str());

        // Without a filter, both rooms match.
        let unfiltered = index.search("matrix", 10, None, None).unwrap();
        assert_eq!(unfiltered.len(), 2);

        // With a filter, only the bookmark in room A is returned.
        let results = index.search("matrix", 10, None, Some(room_a)).unwrap();

        assert_eq!(results.len(), 1, "only the bookmark in the filtered room should be returned");
        assert_eq!(results[0].original_event_id, in_a);
        assert_eq!(results[0].target_room_id, room_a);
        assert!(results[0].score > 0.0, "a matching bookmark must have a positive score");
    }

    /// Within a single filtered room, body relevance still drives the ordering:
    /// the more relevant bookmark in the room ranks first, and matching
    /// bookmarks in other rooms do not interfere.
    #[test]
    fn test_search_body_relevance_ordering_within_room_filter() {
        let mut index = BookmarkIndexBuilder::new_in_memory().build();

        let room_a = ruma::room_id!("!room-a:example.org");
        let room_b = ruma::room_id!("!room-b:example.org");

        let many = owned_event_id!("$1-many:example.org");
        let few = owned_event_id!("$2-few:example.org");
        let noise = owned_event_id!("$3-nois:example.org");

        // Two matching bookmarks in room A with differing term frequency...
        index_bookmark_in_room(
            &mut index,
            many.as_str(),
            "matrix matrix matrix matrix",
            room_a.as_str(),
        );
        index_bookmark_in_room(&mut index, few.as_str(), "matrix is a protocol", room_a.as_str());
        // ...and a strongly-matching bookmark in room B that must be filtered out.
        index_bookmark_in_room(
            &mut index,
            noise.as_str(),
            "matrix matrix matrix matrix matrix matrix",
            room_b.as_str(),
        );

        let results = index.search("matrix", 10, None, Some(room_a)).unwrap();

        assert_eq!(results.len(), 2, "only room A bookmarks should be returned");
        assert!(results.iter().all(|bookmark| bookmark.target_room_id == room_a));

        // Relevance ordering is preserved within the filtered room.
        assert_eq!(results[0].original_event_id, many);
        assert_eq!(results[1].original_event_id, few);
        assert!(
            results[0].score > results[1].score,
            "the more relevant bookmark in the room must score higher: {} vs {}",
            results[0].score,
            results[1].score,
        );
    }

    /// A room_id filter pointing at a room without any matching bookmark returns
    /// nothing, even if the query matches bookmarks in other rooms.
    #[test]
    fn test_search_body_room_filter_no_match_returns_empty() {
        let mut index = BookmarkIndexBuilder::new_in_memory().build();

        let room_a = ruma::room_id!("!room-a:example.org");
        let room_b = ruma::room_id!("!room-b:example.org");

        index_bookmark_in_room(&mut index, "$1-a:example.org", "matrix term", room_a.as_str());

        // The query matches a bookmark, but not in room B.
        let results = index.search("matrix", 10, None, Some(room_b)).unwrap();

        assert!(results.is_empty(), "no bookmark in the filtered room matches: {results:?}");
    }

    /// Regression test: looking a bookmark up by its (root) target event id must
    /// return the original event id of the bookmark event, even for event ids that
    /// contain characters that the tantivy query parser would otherwise treat as syntax.
    #[test]
    fn test_get_original_event_id_from_target_event_returns_original_event_id() {
        let mut index = BookmarkIndexBuilder::new_in_memory().build();

        let original_event_id = owned_event_id!("$bookmark-event_id:example.org");
        let target_event_id = owned_event_id!("$some-event_id:example.org");
        let room_id = owned_room_id!("!room:example.org");

        let pointer_info = BookmarkPointerInfo {
            event_id: original_event_id.clone(),
            original_event_id: original_event_id.clone(),
            target_room_id: room_id,
            target_event_id: target_event_id.clone(),
        };
        let content = IndexedBookmarkContent::new(
            "hello world".to_owned(),
            MilliSecondsSinceUnixEpoch(uint!(0)),
            owned_user_id!("@alice:example.org"),
        );

        let mut writer = index.get_writer().unwrap();
        index.add(&mut writer, pointer_info, content).unwrap();
        index.commit_and_reload(&mut writer).unwrap();

        assert!(index.contains_bookmark(&target_event_id));
        assert_eq!(
            index.get_pointer_info_from_target_event(&target_event_id).map(|o| o.original_event_id),
            Some(original_event_id)
        );

        // A non-bookmarked event returns nothing.
        assert!(!index.contains_bookmark(event_id!("$not-bookmarked:example.org")));
        assert_eq!(
            index.get_pointer_info_from_target_event(event_id!("$not-bookmarked:example.org")),
            None
        );
    }
}
