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
        BookmarkContent, BookmarkIndex, BookmarkIndexOperation, IndexedBookmark,
        builder::BookmarkIndexBuilder,
    },
    error::IndexError,
};
use ruma::{
    EventId, OwnedEventId, RoomId,
    events::{
        AnySyncMessageLikeEvent,
        bookmark::SyncBookmarkEvent,
        room::{
            message::{MessageType, OriginalSyncRoomMessageEvent, Relation, SyncRoomMessageEvent},
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

    /// Handle a [`BookmarkIndexOperation`] in the [`BookmarkIndex`]
    ///
    /// This which will add/remove/edit an event in the index based on the
    /// event type.
    ///
    /// Prefer [`BookmarkIndexGuard::bulk_execute`] for multiple operations.
    pub(crate) fn execute(&mut self, operation: BookmarkIndexOperation) -> Result<(), IndexError> {
        if let Some(index) = self.index.as_mut() {
            index.execute(operation)
        } else {
            let mut index = self.create_index()?;
            index.execute(operation)
        }
    }

    /// Handle a [`BookmarkIndexOperation`] in the [`BookmarkIndex`]
    ///
    /// This which will add/remove/edit an event in the index based on the
    /// event type.
    pub(crate) fn bulk_execute(
        &mut self,
        operations: Vec<BookmarkIndexOperation>,
    ) -> Result<(), IndexError> {
        if let Some(index) = self.index.as_mut() {
            index.bulk_execute(operations)
        } else {
            let mut index = self.create_index()?;
            index.bulk_execute(operations)
        }
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
        if let Some(index) = self.index.as_ref() {
            index.search(query, max_number_of_results, pagination_offset, room_id_filter)
        } else {
            let index = self.create_index()?;
            index.search(query, max_number_of_results, pagination_offset, room_id_filter)
        }
    }

    /// Check if the bookmark index contains an event.
    /// Never fails, and creates the index in memory if
    /// it hasn't been created before.
    pub(crate) fn contains_message(&self, original_event_id: &EventId) -> bool {
        if let Some(index) = self.index.as_ref() {
            index.contains_bookmark(original_event_id)
        } else {
            match self.create_index() {
                Ok(index) => index.contains_bookmark(original_event_id),
                Err(err) => {
                    warn!(
                        "Failed to open the bookmark index, assuming the event isn't bookmarked: {err}"
                    );
                    false
                }
            }
        }
    }

    /// Check if the bookmark index contains an event and return its
    /// bookmark_event_id if its the case.
    /// Never fails, and creates the index in memory if
    /// it hasn't been created before.
    pub(crate) fn get_bookmark_id_for_event(
        &self,
        original_event_id: &EventId,
    ) -> Option<OwnedEventId> {
        if let Some(index) = self.index.as_ref() {
            index.get_bookmark_id_for_event(original_event_id)
        } else {
            match self.create_index() {
                Ok(index) => index.get_bookmark_id_for_event(original_event_id),
                Err(err) => {
                    warn!(
                        "Failed to open the bookmark index, assuming there is no bookmark for that event: {err}"
                    );
                    None
                }
            }
        }
    }

    /// Given a [`TimelineEvent`] this function will check if it is tracked
    /// by a bookmark, derive a [`BookmarkIndexOperation`] if needed and
    /// apply it to the index. This should be used on regular rooms only.
    ///
    /// Prefer [`BookmarkIndexGuard::bulk_handle_timeline_event`] for multiple
    /// events.
    pub async fn handle_timeline_event(
        &mut self,
        event: TimelineEvent,
        room_cache: &RoomEventCache,
        redaction_rules: &RedactionRules,
    ) -> Result<(), IndexError> {
        if let Some(index_operation) =
            self.parse_room_timeline_event(room_cache, event, redaction_rules).await
        {
            self.execute(index_operation)
        } else {
            Ok(())
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
        event: TimelineEvent,
        redaction_rules: &RedactionRules,
    ) -> Result<(), IndexError> {
        if let Some(index_operation) =
            parse_bookmarks_room_event(client, event, redaction_rules).await
        {
            self.execute(index_operation)
        } else {
            Ok(())
        }
    }

    /// Run [`BookmarkIndexGuard::handle_timeline_event`] for multiple
    /// [`TimelineEvent`].
    pub async fn bulk_handle_timeline_event<T>(
        &mut self,
        events: T,
        room_cache: &RoomEventCache,
        redaction_rules: &RedactionRules,
    ) -> Result<(), IndexError>
    where
        T: Iterator<Item = TimelineEvent>,
    {
        let futures =
            events.map(|ev| self.parse_room_timeline_event(room_cache, ev, redaction_rules));

        let operations: Vec<_> = join_all(futures).await.into_iter().flatten().collect();

        self.bulk_execute(operations)
    }

    /// Run [`BookmarkIndexGuard::handle_bookmark_event`] for multiple
    /// [`TimelineEvent`].
    pub async fn bulk_handle_bookmark_event<T>(
        &mut self,
        events: T,
        client: &Client,
        redaction_rules: &RedactionRules,
    ) -> Result<(), IndexError>
    where
        T: Iterator<Item = TimelineEvent>,
    {
        let futures = events.map(|ev| parse_bookmarks_room_event(client, ev, redaction_rules));

        let operations: Vec<_> = join_all(futures).await.into_iter().flatten().collect();

        self.bulk_execute(operations)
    }

    /// Prepare a [`TimelineEvent`] into a [`BookmarkIndexOperation`] for bookmark
    /// indexing.
    async fn parse_room_timeline_event(
        &self,
        cache: &RoomEventCache,
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
                    AnySyncMessageLikeEvent::RoomMessage(event) => {
                        self.handle_room_message(event, cache).await
                    }
                    AnySyncMessageLikeEvent::RoomRedaction(event) => {
                        self.handle_room_redaction(event, redaction_rules)
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

    /// Check if a room message is bookmarked and return a
    /// [`BookmarkIndexOperation::Edit`] if needed.
    async fn handle_room_message(
        &self,
        event: SyncRoomMessageEvent,
        cache: &RoomEventCache,
    ) -> Option<BookmarkIndexOperation> {
        // If the event has a "m.replace" relation, then we want to check the
        // replaced event instead, as we use the root event_id as a deletion key
        // in the index.
        if let Some(event) = event.as_original()
            && self.contains_message(get_root_event_id(event))
        {
            return handle_possible_edit(event, cache).await.or(get_most_recent_edit(
                cache,
                &event.event_id,
                None,
            )
            .await
            .map(|latest_event| {
                BookmarkIndexOperation::Edit(
                    get_root_event_id(&latest_event).to_owned(),
                    convert_room_message_into_bookmark_content(latest_event),
                )
            }));
        }
        // The event is either a redaction or isn't a bookmark.
        // No operation is needed.
        None
    }

    /// Return a [`BookmarkIndexOperation::Edit`] or [`BookmarkIndexOperation::Remove`]
    /// depending on the message.
    fn handle_room_redaction(
        &self,
        event: SyncRoomRedactionEvent,
        rules: &RedactionRules,
    ) -> Option<BookmarkIndexOperation> {
        if let Some(redacted_event_id) = event.redacts(rules)
        // We check before that the event was bookmarked
            && self.contains_message(redacted_event_id)
        {
            // TODO: We remove redacted messages from the bookmark index, but
            // should we also send a redaction for the bookmark event
            // itself ?
            Some(BookmarkIndexOperation::Remove(redacted_event_id.to_owned()))
        } else {
            None
        }
    }
}

/// This eventually walks up the relation chain of a sync event
/// and returns the event_id of the original event.
fn get_root_event_id(event: &OriginalSyncRoomMessageEvent) -> &OwnedEventId {
    if let Some(Relation::Replacement(ref replacement_data)) = event.content.relates_to {
        &replacement_data.event_id
    } else {
        &event.event_id
    }
}

/// Given an event id this function returns the most recent edit on said event
/// or the event itself if there are no edits.
/// If a Client is provided, this function will try to fetch the event if it
/// hasn't been found.
async fn get_most_recent_edit(
    cache: &RoomEventCache,
    original: &EventId,
    client: Option<(&Client, &RoomId)>,
) -> Option<OriginalSyncRoomMessageEvent> {
    use ruma::events::{AnySyncTimelineEvent, relation::RelationType};

    let (original_ev, related) = if let Ok(Some((original_ev, related))) =
        cache.find_event_with_relations(original, Some(vec![RelationType::Replacement])).await
    {
        (original_ev, related)
    } else if let Some((client, room_id)) = client
        && let Some(room) = client.get_room(room_id)
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

/// If the given [`OriginalSyncRoomMessageEvent`] is an edit we make an
/// [`BookmarkIndexOperation::Edit`] with the new most recent version of the
/// original.
async fn handle_possible_edit(
    event: &OriginalSyncRoomMessageEvent,
    cache: &RoomEventCache,
) -> Option<BookmarkIndexOperation> {
    if let Some(Relation::Replacement(replacement_data)) = &event.content.relates_to {
        if let Some(recent) = get_most_recent_edit(cache, &replacement_data.event_id, None).await {
            return Some(BookmarkIndexOperation::Edit(
                replacement_data.event_id.clone(),
                convert_room_message_into_bookmark_content(recent),
            ));
        } else {
            return Some(BookmarkIndexOperation::Noop);
        }
    }
    None
}

/// Fetch the referenced bookmarked event and return a
/// [`BookmarkIndexOperation::Add`].
async fn handle_sync_bookmark(
    bookmark_event: SyncBookmarkEvent,
    client: &Client,
) -> Option<BookmarkIndexOperation> {
    match bookmark_event {
        SyncBookmarkEvent::Original(bookmark) => {
            if let Ok((cache, _)) =
                client.event_cache().for_room(&bookmark.content.pointer.room_id).await
                && let Some(content) = get_most_recent_edit(
                    &cache,
                    &bookmark.content.pointer.event_id,
                    Some((client, &bookmark.content.pointer.room_id)),
                )
                .await
            {
                Some(BookmarkIndexOperation::Add(
                    bookmark.to_owned(),
                    convert_room_message_into_bookmark_content(content),
                ))
            } else {
                warn!("Couldn't find pointed event for bookmark.");
                None
            }
        }
        SyncBookmarkEvent::Redacted(redacted) => {
            Some(BookmarkIndexOperation::RemoveWithBookmarkId(redacted.event_id))
        }
    }
}

fn convert_room_message_into_bookmark_content(
    message: OriginalSyncRoomMessageEvent,
) -> BookmarkContent {
    let body = match &message.content.msgtype {
        MessageType::Text(content) => Some(content.body.clone()),
        MessageType::Notice(content) => Some(content.body.clone()),
        MessageType::Emote(content) => Some(content.body.clone()),
        MessageType::Audio(content) if let Some(caption) = content.caption() => {
            Some(caption.to_owned())
        }
        MessageType::File(content) if let Some(caption) = content.caption() => {
            Some(caption.to_owned())
        }
        MessageType::Image(content) if let Some(caption) = content.caption() => {
            Some(caption.to_owned())
        }
        MessageType::Video(content) if let Some(caption) = content.caption() => {
            Some(caption.to_owned())
        }
        _ => None,
    };

    BookmarkContent::new(message.event_id, body, message.origin_server_ts, message.sender)
}

/// Return a [`BookmarkIndexOperation::Remove`] or nothing
/// depending on the message.
fn handle_bookmark_redaction(
    event: SyncRoomRedactionEvent,
    rules: &RedactionRules,
) -> Option<BookmarkIndexOperation> {
    event.redacts(rules).map(|id| BookmarkIndexOperation::RemoveWithBookmarkId(id.to_owned()))
}

/// Prepare a [`TimelineEvent`] of the `m.bookmarks` room into a
/// [`BookmarkIndexOperation`] for bookmark indexing.
async fn parse_bookmarks_room_event(
    client: &Client,
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
                    handle_bookmark_redaction(event, redaction_rules)
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

// #[cfg(test)]
// mod tests {
//     use matrix_sdk_test::{JoinedRoomBuilder, async_test, event_factory::EventFactory};
//     use ruma::{
//         event_id, events::room::message::RoomMessageEventContentWithoutRelation, room_id, user_id,
//     };

//     use crate::test_utils::mocks::MatrixMockServer;

//     #[cfg(feature = "experimental-search")]
//     #[async_test]
//     async fn test_sync_message_is_indexed() {
//         let mock_server = MatrixMockServer::new().await;
//         let client = mock_server.client_builder().build().await;

//         client.event_cache().subscribe().unwrap();

//         let room_id = room_id!("!room_id:localhost");
//         let event_id = event_id!("$event_id:localost");
//         let user_id = user_id!("@user_id:localost");

//         let event_factory = EventFactory::new();
//         let room = mock_server
//             .sync_room(
//                 &client,
//                 JoinedRoomBuilder::new(room_id).add_timeline_bulk(vec![
//                     event_factory
//                         .text_msg("this is a sentence")
//                         .event_id(event_id)
//                         .sender(user_id)
//                         .into_raw_sync(),
//                 ]),
//             )
//             .await;

//         let response = room.search("this", 5, None).await.expect("search should have 1 result");

//         assert_eq!(response.len(), 1, "unexpected numbers of responses: {response:?}");
//         assert_eq!(response[0], event_id, "event id doesn't match: {response:?}");
//     }

//     #[cfg(feature = "experimental-search")]
//     #[async_test]
//     async fn test_search_index_edit_ordering() {
//         let room_id = room_id!("!room_id:localhost");
//         let dummy_id = event_id!("$dummy");
//         let edit1_id = event_id!("$edit1");
//         let edit2_id = event_id!("$edit2");
//         let edit3_id = event_id!("$edit3");
//         let original_id = event_id!("$original");

//         let server = MatrixMockServer::new().await;
//         let client = server.client_builder().build().await;

//         let event_cache = client.event_cache();
//         event_cache.subscribe().unwrap();

//         let room = server.sync_joined_room(&client, room_id).await;

//         let f = EventFactory::new().room(room_id).sender(user_id!("@user_id:localhost"));

//         // Indexable dummy message required because BookmarkIndex is initialised lazily.
//         let dummy = f.text_msg("dummy").event_id(dummy_id);

//         let original = f.text_msg("This is a message").event_id(original_id);

//         let edit1 = f
//             .text_msg("* A new message")
//             .edit(original_id, RoomMessageEventContentWithoutRelation::text_plain("A new message"))
//             .event_id(edit1_id);

//         let edit2 = f
//             .text_msg("* An even newer message")
//             .edit(
//                 original_id,
//                 RoomMessageEventContentWithoutRelation::text_plain("An even newer message"),
//             )
//             .event_id(edit2_id);

//         let edit3 = f
//             .text_msg("* The newest message")
//             .edit(
//                 original_id,
//                 RoomMessageEventContentWithoutRelation::text_plain("The newest message"),
//             )
//             .event_id(edit3_id);

//         server
//             .sync_room(
//                 &client,
//                 JoinedRoomBuilder::new(room_id)
//                     .add_timeline_event(dummy)
//                     .add_timeline_event(edit1)
//                     .add_timeline_event(edit2),
//             )
//             .await;

//         let results = room.search("message", 3, None).await.unwrap();

//         assert_eq!(results.len(), 0, "Search should return 0 results, got {results:?}");

//         // Adding the original after some pending edits should add the latest edit
//         // instead of the original.
//         server
//             .sync_room(&client, JoinedRoomBuilder::new(room_id).add_timeline_event(original))
//             .await;

//         let results = room.search("message", 3, None).await.unwrap();

//         assert_eq!(results.len(), 1, "Search should return 1 result, got {results:?}");
//         assert_eq!(results[0], edit2_id, "Search should return latest edit, got {:?}", results[0]);

//         // Editing the original after it exists and there has been another edit should
//         // delete the previous edits and add this one
//         server.sync_room(&client, JoinedRoomBuilder::new(room_id).add_timeline_event(edit3)).await;

//         let results = room.search("message", 3, None).await.unwrap();

//         assert_eq!(results.len(), 1, "Search should return 1 result, got {results:?}");
//         assert_eq!(results[0], edit3_id, "Search should return latest edit, got {:?}", results[0]);
//     }
// }
