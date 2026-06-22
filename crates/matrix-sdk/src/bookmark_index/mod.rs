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

//! The bookmark index is an abstraction layer in the matrix-sdk for the
//! bookmark index, that is defined in the matrix-sdk-search crate.
//! It provides a single [`BookmarkIndex`] which is a single entry point
//! for managing bookmarks from every room.

use std::{path::PathBuf, sync::Arc};

use futures_util::future::join_all;
use matrix_sdk_base::deserialized_responses::TimelineEvent;
use matrix_sdk_search::{
    bookmarks::{
        BookmarkIndex, BookmarkIndexOperation, BookmarkPointerInfo, IndexedBookmark,
        builder::BookmarkIndexBuilder,
    },
    error::IndexError,
};
use ruma::{
    EventId, RoomId,
    events::{
        AnySyncMessageLikeEvent, AnySyncTimelineEvent,
        bookmark::{OriginalSyncBookmarkEvent, SyncBookmarkEvent},
        room::{
            message::{OriginalSyncRoomMessageEvent, Relation},
            redaction::SyncRoomRedactionEvent,
        },
    },
    room_version_rules::RedactionRules,
};
use tokio::sync::{Mutex, MutexGuard};
use tracing::{debug, warn};

use crate::{Client, event_cache::RoomEventCache};

type Password = String;

/// Type of location to store [`BookmarkIndex`]
#[derive(Clone, Debug)]
pub enum BookmarkIndexStoreKind {
    /// Store unencrypted in file system folder
    UnencryptedDirectory(PathBuf),
    /// Store encrypted in file system folder
    EncryptedDirectory(PathBuf, Password),
    /// Store in memory
    InMemory,
}

/// Object that handles interactions with [`BookmarkIndex`] for search
#[derive(Clone, Debug)]
pub struct BookmarkIndexStore {
    /// Mutex to the bookmark index
    bookmark_index: Arc<Mutex<Option<BookmarkIndex>>>,

    /// Base directory that stores the directories for each BookmarkIndex
    search_index_store_kind: BookmarkIndexStoreKind,
}

impl BookmarkIndexStore {
    /// Create a new [`BookmarkIndexStore`]
    pub fn new(
        bookmark_index: Arc<Mutex<Option<BookmarkIndex>>>,
        search_index_store_kind: BookmarkIndexStoreKind,
    ) -> Self {
        Self { bookmark_index, search_index_store_kind }
    }

    /// Acquire [`BookmarkIndexGuard`] for this [`BookmarkIndex`].
    pub async fn lock(&self) -> BookmarkIndexGuard<'_> {
        BookmarkIndexGuard {
            index: self.bookmark_index.lock().await,
            bookmark_index_store_kind: &self.search_index_store_kind,
        }
    }
}

/// Object that represents an acquired [`BookmarkIndexStore`].
#[derive(Debug)]
pub struct BookmarkIndexGuard<'a> {
    /// Guard around the [`BookmarkIndex`]
    index: MutexGuard<'a, Option<BookmarkIndex>>,

    /// Base directory that stores the directories for each BookmarkIndex
    bookmark_index_store_kind: &'a BookmarkIndexStoreKind,
}

impl BookmarkIndexGuard<'_> {
    fn create_index(&self) -> Result<BookmarkIndex, IndexError> {
        let index = match self.bookmark_index_store_kind {
            BookmarkIndexStoreKind::UnencryptedDirectory(path) => {
                BookmarkIndexBuilder::new_on_disk(path.to_path_buf()).unencrypted().build()?
            }
            BookmarkIndexStoreKind::EncryptedDirectory(path, password) => {
                BookmarkIndexBuilder::new_on_disk(path.to_path_buf()).encrypted(password).build()?
            }
            BookmarkIndexStoreKind::InMemory => BookmarkIndexBuilder::new_in_memory().build(),
        };
        Ok(index)
    }

    /// Return a mutable reference to the [`BookmarkIndex`], creating and storing
    /// it first if it hasn't been initialized yet.
    fn get_or_create_index(&mut self) -> Result<&mut BookmarkIndex, IndexError> {
        if self.index.is_none() {
            let index = self.create_index()?;
            *self.index = Some(index);
        }

        Ok(self.index.as_mut().expect("index should exist"))
    }

    /// Handle a [`BookmarkIndexOperation`] in the [`BookmarkIndex`]
    ///
    /// This which will add/remove/edit an event in the index based on the
    /// event type.
    ///
    /// Prefer [`BookmarkIndexGuard::bulk_execute`] for multiple operations.
    pub(crate) fn execute(&mut self, operation: BookmarkIndexOperation) -> Result<(), IndexError> {
        self.get_or_create_index()?.execute(operation)
    }

    /// Handle a [`BookmarkIndexOperation`] in the [`BookmarkIndex`]
    ///
    /// This which will add/remove/edit an event in the index based on the
    /// event type.
    pub(crate) fn bulk_execute(
        &mut self,
        operations: Vec<BookmarkIndexOperation>,
    ) -> Result<(), IndexError> {
        self.get_or_create_index()?.bulk_execute(operations)
    }

    /// Search the global bookmark index for the query and return at most
    /// max_number_of_results results. A room_id filter may be passed to
    /// get bookmarks of one room only.
    pub(crate) fn search(
        &mut self,
        query: &str,
        max_number_of_results: usize,
        pagination_offset: Option<usize>,
        room_id_filter: Option<&RoomId>,
    ) -> Result<Vec<IndexedBookmark>, IndexError> {
        self.get_or_create_index()?.search(
            query,
            max_number_of_results,
            pagination_offset,
            room_id_filter,
        )
    }

    /// Check if the bookmark index contains an event and return its
    /// bookmark original_event_id if its the case.
    /// The checked event_id should be the root/original one if
    /// the event has `m.replace` relation(s).
    /// Never fails, and creates the index in memory if
    /// it hasn't been created before.
    pub(crate) fn get_bookmark_id_for_event(
        &self,
        original_target_event_id: &EventId,
    ) -> Option<BookmarkPointerInfo> {
        if let Some(index) = self.index.as_ref() {
            index.get_pointer_info_from_target_event(original_target_event_id)
        } else {
            match self.create_index() {
                Ok(index) => index.get_pointer_info_from_target_event(original_target_event_id),
                Err(err) => {
                    warn!(
                        "Failed to open the bookmark index, assuming there is no bookmark for that event: {err}"
                    );
                    None
                }
            }
        }
    }

    /// Given a [`TimelineEvent`] of a `m.bookmarks` room this function will derive a
    /// [`BookmarkIndexOperation`], if it should be handled, and execute it;
    /// returning the result.
    ///
    /// Prefer [`BookmarkIndexGuard::bulk_handle_bookmark_event`] for multiple
    /// events.
    pub async fn handle_bookmark_event(
        &mut self,
        client: &Client,
        room_cache: &RoomEventCache,
        event: TimelineEvent,
        redaction_rules: &RedactionRules,
    ) -> Result<(), IndexError> {
        if let Some(index_operation) =
            parse_bookmarks_room_event(client, room_cache, event, redaction_rules).await
        {
            self.execute(index_operation)
        } else {
            Ok(())
        }
    }

    /// Run [`BookmarkIndexGuard::handle_bookmark_event`] for multiple
    /// [`TimelineEvent`].
    pub async fn bulk_handle_bookmark_event<T>(
        &mut self,
        events: T,
        client: &Client,
        room_cache: &RoomEventCache,
        redaction_rules: &RedactionRules,
    ) -> Result<(), IndexError>
    where
        T: Iterator<Item = TimelineEvent>,
    {
        let futures =
            events.map(|ev| parse_bookmarks_room_event(client, room_cache, ev, redaction_rules));

        let operations: Vec<_> = join_all(futures).await.into_iter().flatten().collect();

        self.bulk_execute(operations)
    }
}

/// Given an event id this function returns the most recent edit on said event
/// or the event itself if there are no edits.
/// If a Client is provided, this function will try to fetch the event if it
/// hasn't been found.
async fn get_most_recent_timeline_event_edit(
    target_room_cache: &RoomEventCache,
    original: &EventId,
    client: &Client,
    target_room_id: &RoomId,
) -> Option<OriginalSyncRoomMessageEvent> {
    use ruma::events::{AnySyncTimelineEvent, relation::RelationType};

    let (original_ev, related) = if let Ok(Some((original_ev, related))) = target_room_cache
        .find_event_with_relations(original, Some(vec![RelationType::Replacement]))
        .await
    {
        (original_ev, related)
    } else if let Some(room) = client.get_room(target_room_id)
        && let Ok((original_ev, related)) = room
            .load_or_fetch_event_with_relations(
                original,
                Some(vec![RelationType::Replacement]),
                None,
            )
            .await
    {
        (original_ev, related)
    } else {
        debug!("Couldn't find relations for {}", original);
        return None;
    };

    match related.last().unwrap_or(&original_ev).raw().deserialize() {
        Ok(AnySyncTimelineEvent::MessageLike(AnySyncMessageLikeEvent::RoomMessage(latest))) => {
            latest.as_original().cloned()
        }
        _ => None,
    }
}

/// Fetch the referenced bookmarked event and return a
/// [`BookmarkIndexOperation::Add`].
async fn handle_sync_bookmark(
    bookmark_event: SyncBookmarkEvent,
    client: &Client,
) -> Option<BookmarkIndexOperation> {
    match bookmark_event {
        SyncBookmarkEvent::Original(bookmark) => {
            if let Some(op) = handle_possible_edit(&bookmark, client).await {
                Some(op)
            } else {
                if let Ok((cache, _)) =
                    client.event_cache().room(&bookmark.content.pointer.room_id).await
                    && let Some(content) = get_most_recent_timeline_event_edit(
                        &cache,
                        &bookmark.content.pointer.event_id,
                        client,
                        &bookmark.content.pointer.room_id,
                    )
                    .await
                {
                    Some(BookmarkIndexOperation::Add(Box::new(bookmark.to_owned()), content.into()))
                } else {
                    warn!("Couldn't get event cache for targeted room_id.");
                    None
                }
            }
        }
        SyncBookmarkEvent::Redacted(redacted) => {
            Some(BookmarkIndexOperation::Remove(redacted.event_id))
        }
    }
}

/// If the given [`OriginalSyncBookmarkEvent`] is an edit we make an
/// [`BookmarkIndexOperation::Edit`] with the new most recent version of the
/// original.
async fn handle_possible_edit(
    event: &OriginalSyncBookmarkEvent,
    client: &Client,
) -> Option<BookmarkIndexOperation> {
    if let Some(Relation::Replacement(replacement_data)) = &event.content.relates_to {
        if let Ok((cache, _)) =
            client.event_cache().room(&replacement_data.new_content.pointer.room_id).await
            && let Some(recent) = get_most_recent_timeline_event_edit(
                &cache,
                &replacement_data.event_id,
                client,
                &replacement_data.new_content.pointer.room_id,
            )
            .await
        {
            return Some(BookmarkIndexOperation::Edit(event.clone().into(), recent.into()));
        } else {
            return Some(BookmarkIndexOperation::Noop);
        }
    }
    None
}

/// Return a [`BookmarkIndexOperation::Remove`] or nothing
/// depending on the message.
async fn handle_bookmark_redaction(
    event: SyncRoomRedactionEvent,
    cache: &RoomEventCache,
    rules: &RedactionRules,
    client: &Client,
) -> Option<BookmarkIndexOperation> {
    if let Some(redacted_event_id) = event.redacts(rules)
        && let Ok(Some(redacted_event)) = cache.find_event(redacted_event_id).await
        && let Ok(AnySyncTimelineEvent::MessageLike(AnySyncMessageLikeEvent::Bookmark(
            redacted_event,
        ))) = redacted_event.raw().deserialize()
        && let SyncBookmarkEvent::Original(redacted_event) = redacted_event
    {
        return handle_possible_edit(&redacted_event, client)
            .await
            .or(Some(BookmarkIndexOperation::Remove(redacted_event.event_id)));
    }
    None
}

/// Prepare a [`TimelineEvent`] of the `m.bookmarks` room into a
/// [`BookmarkIndexOperation`] for bookmark indexing.
async fn parse_bookmarks_room_event(
    client: &Client,
    bookmarks_room_cache: &RoomEventCache,
    event: TimelineEvent,
    redaction_rules: &RedactionRules,
) -> Option<BookmarkIndexOperation> {
    use ruma::events::AnySyncTimelineEvent;

    if event.kind.is_utd() {
        return None;
    }

    match event.raw().deserialize() {
        Ok(event) => match event {
            AnySyncTimelineEvent::MessageLike(event) => match event {
                AnySyncMessageLikeEvent::Bookmark(event) => {
                    handle_sync_bookmark(event, client).await
                }
                AnySyncMessageLikeEvent::RoomRedaction(event) => {
                    handle_bookmark_redaction(event, bookmarks_room_cache, redaction_rules, client)
                        .await
                }
                _ => None,
            },
            AnySyncTimelineEvent::State(_) => None,
        },

        Err(e) => {
            warn!("failed to parse event: {e:?}");
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use matrix_sdk_search::bookmarks::IndexedBookmark;
    use matrix_sdk_test::{JoinedRoomBuilder, async_test, event_factory::EventFactory};
    use ruma::{
        EventId, RoomVersionId, event_id,
        events::{
            bookmark::{BookmarkEventContent, PointerContentBlock},
            room::message::RoomMessageEventContentWithoutRelation,
        },
        room_id, user_id,
    };

    use crate::{Room, test_utils::mocks::MatrixMockServer};

    /// Bookmark indexing happens in a background task, so poll the index until
    /// the bookmark pointing to `expected_event_id` shows up (or time out).
    async fn wait_for_bookmark(
        room: &Room,
        query: &str,
        expected_event_id: &EventId,
    ) -> Vec<IndexedBookmark> {
        for _ in 0..100 {
            let results = room.search_room_bookmarks(query, 5, None).await.unwrap();
            if results.iter().any(|bookmark| bookmark.event_id == expected_event_id) {
                return results;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        panic!("timed out waiting for the bookmark of {expected_event_id} to be indexed");
    }

    /// Like [`wait_for_bookmark`], but waits until at least `count` bookmarks
    /// match the query (or times out).
    async fn wait_for_bookmark_count(
        room: &Room,
        query: &str,
        count: usize,
    ) -> Vec<IndexedBookmark> {
        for _ in 0..100 {
            let results = room.search_room_bookmarks(query, 10, None).await.unwrap();
            if results.len() >= count {
                return results;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        panic!("timed out waiting for {count} bookmarks to be indexed");
    }

    #[async_test]
    async fn test_sync_bookmark_is_indexed() {
        let mock_server = MatrixMockServer::new().await;
        let client = mock_server.client_builder().build().await;

        client.event_cache().subscribe().unwrap();

        let room_id = room_id!("!room_id:localhost");
        let bookmarks_room_id = room_id!("!bookmarks_room_id:localhost");
        let event_id = event_id!("$event_id:localhost");
        let bookmark_event_id = event_id!("$bookmark_event_id:localhost");
        let user_id = user_id!("@user_id:localhost");

        let f = EventFactory::new().sender(user_id);

        // Sync the regular room that contains the message we are going to bookmark.
        let room = mock_server
            .sync_room(
                &client,
                JoinedRoomBuilder::new(room_id).add_timeline_bulk(vec![
                    f.text_msg("this is a sentence")
                        .event_id(event_id)
                        .room(room_id)
                        .into_raw_sync(),
                ]),
            )
            .await;

        // Sync the bookmarks room with a bookmark event that points to the message.
        let pointer = PointerContentBlock::new(room_id.to_owned(), event_id.to_owned(), vec![]);
        let bookmark = f
            .event(BookmarkEventContent::new(pointer, "user", "room"))
            .event_id(bookmark_event_id)
            .room(bookmarks_room_id);

        mock_server
            .sync_room(
                &client,
                JoinedRoomBuilder::new(bookmarks_room_id)
                    .add_state_event(f.create(user_id, RoomVersionId::V12).with_bookmarks_type())
                    .add_timeline_event(bookmark),
            )
            .await;

        let response = wait_for_bookmark(&room, "this", event_id).await;

        assert_eq!(response.len(), 1, "unexpected numbers of responses: {response:?}");
        assert_eq!(
            response[0].original_event_id, event_id,
            "bookmarked event id doesn't match: {response:?}"
        );
        assert_eq!(
            response[0].target_event_id, bookmark_event_id,
            "bookmark pointer event id doesn't match: {response:?}"
        );
    }

    #[async_test]
    async fn test_bookmark_index_edit_ordering() {
        let room_id = room_id!("!room_id:localhost");
        let bookmarks_room_id = room_id!("!bookmarks_room_id:localhost");
        let bookmark_event_id = event_id!("$bookmark_event_id:localhost");
        let edit1_id = event_id!("$edit1");
        let edit2_id = event_id!("$edit2");
        let edit3_id = event_id!("$edit3");
        let original_id = event_id!("$original");
        let user_id = user_id!("@user_id:localhost");

        let server = MatrixMockServer::new().await;
        let client = server.client_builder().build().await;

        let event_cache = client.event_cache();
        event_cache.subscribe().unwrap();

        let room = server.sync_joined_room(&client, room_id).await;

        let f = EventFactory::new().room(room_id).sender(user_id);

        let original = f.text_msg("This is a message").event_id(original_id);

        let edit1 = f
            .text_msg("* A new message")
            .edit(original_id, RoomMessageEventContentWithoutRelation::text_plain("A new message"))
            .event_id(edit1_id);

        let edit2 = f
            .text_msg("* An even newer message")
            .edit(
                original_id,
                RoomMessageEventContentWithoutRelation::text_plain("An even newer message"),
            )
            .event_id(edit2_id);

        let edit3 = f
            .text_msg("* The newest message")
            .edit(
                original_id,
                RoomMessageEventContentWithoutRelation::text_plain("The newest message"),
            )
            .event_id(edit3_id);

        // The original message and two edits are already in the room before it gets
        // bookmarked.
        server
            .sync_room(
                &client,
                JoinedRoomBuilder::new(room_id)
                    .add_timeline_event(original)
                    .add_timeline_event(edit1)
                    .add_timeline_event(edit2),
            )
            .await;

        // Nothing is bookmarked yet, so there are no results.
        let results = room.search_room_bookmarks("message", 3, None).await.unwrap();
        assert_eq!(results.len(), 0, "Search should return 0 results, got {results:?}");

        // Bookmarking the original after some edits should index the latest edit
        // instead of the original.
        let pointer = PointerContentBlock::new(room_id.to_owned(), original_id.to_owned(), vec![]);
        let bookmark = f
            .event(BookmarkEventContent::new(pointer, "user", "room"))
            .event_id(bookmark_event_id)
            .room(bookmarks_room_id);

        server
            .sync_room(
                &client,
                JoinedRoomBuilder::new(bookmarks_room_id)
                    .add_state_event(f.create(user_id, RoomVersionId::V12).with_bookmarks_type())
                    .add_timeline_event(bookmark),
            )
            .await;

        let results = wait_for_bookmark(&room, "message", edit2_id).await;

        assert_eq!(results.len(), 1, "Search should return 1 result, got {results:?}");
        assert_eq!(
            results[0].event_id, edit2_id,
            "Search should return latest edit, got {:?}",
            results[0]
        );
        assert_eq!(
            results[0].original_event_id, original_id,
            "Search should keep the original event id, got {:?}",
            results[0]
        );

        // Editing the original after it has been bookmarked should delete the previous
        // edits and index this one.
        server.sync_room(&client, JoinedRoomBuilder::new(room_id).add_timeline_event(edit3)).await;

        let results = wait_for_bookmark(&room, "message", edit3_id).await;

        assert_eq!(results.len(), 1, "Search should return 1 result, got {results:?}");
        assert_eq!(
            results[0].event_id, edit3_id,
            "Search should return latest edit, got {:?}",
            results[0]
        );
        assert_eq!(
            results[0].original_event_id, original_id,
            "Search should keep the original event id, got {:?}",
            results[0]
        );
    }

    #[async_test]
    async fn test_bookmark_search_relevancy() {
        let room_id = room_id!("!room_id:localhost");
        let bookmarks_room_id = room_id!("!bookmarks_room_id:localhost");
        let user_id = user_id!("@user_id:localhost");

        // The event holding the search term once, and the bookmark pointing to it.
        let low_id = event_id!("$low");
        let low_bookmark_id = event_id!("$low_bookmark");
        // The event holding the search term several times, and its bookmark.
        let high_id = event_id!("$high");
        let high_bookmark_id = event_id!("$high_bookmark");

        let server = MatrixMockServer::new().await;
        let client = server.client_builder().build().await;

        client.event_cache().subscribe().unwrap();

        let f = EventFactory::new().sender(user_id);

        // Two bookmarkable messages: both mention "matrix", but one mentions it more
        // often, so it should score higher.
        let room = server
            .sync_room(
                &client,
                JoinedRoomBuilder::new(room_id).add_timeline_bulk(vec![
                    f.text_msg("matrix is a protocol")
                        .event_id(low_id)
                        .room(room_id)
                        .into_raw_sync(),
                    f.text_msg("matrix matrix matrix is the best matrix")
                        .event_id(high_id)
                        .room(room_id)
                        .into_raw_sync(),
                ]),
            )
            .await;

        // Bookmark both messages in the bookmarks room.
        let low_bookmark = f
            .event(BookmarkEventContent::new(
                PointerContentBlock::new(room_id.to_owned(), low_id.to_owned(), vec![]),
                "user",
                "room",
            ))
            .event_id(low_bookmark_id)
            .room(bookmarks_room_id);

        let high_bookmark = f
            .event(BookmarkEventContent::new(
                PointerContentBlock::new(room_id.to_owned(), high_id.to_owned(), vec![]),
                "user",
                "room",
            ))
            .event_id(high_bookmark_id)
            .room(bookmarks_room_id);

        server
            .sync_room(
                &client,
                JoinedRoomBuilder::new(bookmarks_room_id)
                    .add_state_event(f.create(user_id, RoomVersionId::V12).with_bookmarks_type())
                    .add_timeline_event(low_bookmark)
                    .add_timeline_event(high_bookmark),
            )
            .await;

        let results = wait_for_bookmark_count(&room, "matrix", 2).await;

        assert_eq!(results.len(), 2, "Search should return 2 results, got {results:?}");
        // The message mentioning "matrix" more often must rank first.
        assert_eq!(
            results[0].event_id, high_id,
            "Most relevant bookmark should rank first, got {results:?}"
        );
        assert_eq!(
            results[1].event_id, low_id,
            "Least relevant bookmark should rank last, got {results:?}"
        );
        assert!(
            results[0].score > results[1].score,
            "Scores should be ordered descending, got {results:?}"
        );

        println!("SCORE 1: {}", results[0].score);
        println!("SCORE 2: {}", results[1].score);
    }
}
