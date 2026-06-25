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

//! Message bookmarking facilities and high-level helpers to save / delete
//! bookmarks, or perform searches across one or multiple rooms, with
//! pagination support.

use matrix_sdk_base::{RoomStateFilter, deserialized_responses::TimelineEvent};
#[cfg(doc)]
use matrix_sdk_search::bookmarks::BookmarkIndex;
pub use matrix_sdk_search::{bookmarks::IndexedBookmark, error::IndexError};
use ruma::{
    EventId, OwnedEventId, OwnedRoomId,
    api::client::{
        redact::redact_event,
        room::create_room::{self, v3::CreationContent},
    },
    events::{
        bookmark::{BookmarkEventContent, PointerContentBlock},
        bookmarks_room::BookmarksRoomEventContent,
        relation::RelationType,
    },
    serde::Raw,
};
use std::collections::HashSet;
use tracing::error;

use crate::{Client, Room, message_search::SearchError, room::futures::SendMessageLikeEventResult};

impl Room {
    /// Search the [`BookmarkIndex`]  and return at most
    /// max_number_of_results results.
    pub async fn search_room_bookmarks(
        &self,
        query: &str,
        max_number_of_results: usize,
        pagination_offset: Option<usize>,
    ) -> Result<Vec<IndexedBookmark>, IndexError> {
        let mut index = self.client.bookmark_index().lock().await;
        index.search(query, max_number_of_results, pagination_offset, Some(self.room_id()))
    }

    /// Search for bookmarks in this room matching the given query, returning an
    /// iterator over the results.
    pub fn search_room_bookmarks_iterator(
        &self,
        query: String,
        num_results_per_batch: usize,
    ) -> BookmarkSearchIterator {
        BookmarkSearchIterator {
            room: self.clone(),
            query,
            offset: None,
            is_done: false,
            num_results_per_batch,
        }
    }

    /// Return the set of (root) event ids that are currently bookmarked in
    /// this room.
    ///
    /// The returned event ids are the *original* (root of the `m.replace`
    /// relation chain) event ids, i.e. the ones that match a timeline item's
    /// own event id.
    pub async fn bookmarked_event_ids(&self) -> Result<HashSet<OwnedEventId>, IndexError> {
        // Number of bookmarks to load per index query.
        const BATCH_SIZE: usize = 100;

        let mut event_ids = HashSet::new();
        let mut offset = 0;

        loop {
            let batch = self.search_room_bookmarks("*", BATCH_SIZE, Some(offset)).await?;
            if batch.is_empty() {
                break;
            }
            offset += batch.len();
            event_ids.extend(batch.into_iter().map(|bookmark| bookmark.target_event_id));
        }

        Ok(event_ids)
    }

    /// Bookmark an event of this room
    /// This method will not check the validity of the event_id
    pub async fn bookmark_event(
        &self,
        event_id: &EventId,
        sender_display_name: &str,
    ) -> crate::Result<SendMessageLikeEventResult> {
        // TODO: Fill via parameter ?
        let pointer =
            PointerContentBlock::new(self.room_id().to_owned(), event_id.to_owned(), vec![]);
        let room_display_name =
            self.cached_display_name().map(|name| name.to_string()).unwrap_or_default();
        let payload = BookmarkEventContent::new(pointer, sender_display_name, &room_display_name);
        let send_closure = move |r: Room| async move { r.send(payload).await };
        let maybe_result = self.client().with_bookmarks_room(send_closure, true).await;
        maybe_result.ok_or(crate::Error::BookmarksError)?.await
    }
}

/// An async iterator for a search query in a single room.
#[derive(Debug)]
pub struct BookmarkSearchIterator {
    /// The room in which the search is performed.
    room: Room,

    /// The search query, directly forwarded to the search API.
    query: String,

    /// The current start offset in the search results, or `None` if we haven't
    /// called the iterator yet.
    offset: Option<usize>,

    /// Whether we have exhausted the search results.
    is_done: bool,

    /// Number of results to return (at most) per batch when calling
    /// [`Self::next()`].
    num_results_per_batch: usize,
}

impl BookmarkSearchIterator {
    /// Return the next batch of event IDs matching the search query, or `None`
    /// if there are no more results.
    pub async fn next(&mut self) -> Result<Option<Vec<IndexedBookmark>>, IndexError> {
        if self.is_done {
            return Ok(None);
        }

        let result = self
            .room
            .search_room_bookmarks(&self.query, self.num_results_per_batch, self.offset)
            .await?;

        if result.is_empty() {
            self.is_done = true;
            Ok(None)
        } else {
            self.offset = Some(self.offset.unwrap_or(0) + result.len());
            Ok(Some(result))
        }
    }

    /// Returns [`TimelineEvent`]s instead of event IDs, by loading the events
    /// from the store or from network.
    pub async fn next_events(&mut self) -> Result<Option<Vec<TimelineEvent>>, SearchError> {
        let Some(indexed_bookmarks) = self.next().await? else {
            return Ok(None);
        };
        let mut results = Vec::new();
        for bookmark in indexed_bookmarks {
            let (original_event, mut replacements) = self
                .room
                .load_or_fetch_event_with_relations(
                    &bookmark.target_event_id,
                    Some(vec![RelationType::Replacement]),
                    None,
                )
                .await?;

            results.push(replacements.pop().unwrap_or(original_event));
        }
        Ok(Some(results))
    }
}

impl Client {
    /// Search across the global bookmarks index for events with the given query,
    /// returning a builder for an iterator over the results.
    pub fn search_bookmarks(
        &self,
        query: String,
        num_results_per_batch: usize,
    ) -> GlobalBookmarkSearchBuilder {
        GlobalBookmarkSearchBuilder::new(self.clone(), query, num_results_per_batch)
    }

    /// Retrieve the bookmarks room
    pub async fn get_bookmarks_room(&self) -> crate::Result<Option<Room>> {
        if let Some(bookmarks_room_id) = self.account().get_bookmarks_room_id().await? {
            Ok(self.get_room(&bookmarks_room_id))
        } else {
            Ok(None)
        }
    }

    /// Do an operation with the bookmarks room
    pub async fn with_bookmarks_room<F, R>(&self, f: F, create_if_needed: bool) -> Option<R>
    where
        F: FnOnce(Room) -> R,
    {
        match self.get_bookmarks_room().await {
            Ok(Some(room)) => Some(f(room)),
            Ok(None) if create_if_needed => match create_bookmarks_room(self).await {
                Ok(room) => Some(f(room)),
                Err(e) => {
                    error!("Error while trying to create bookmarks room {e}");
                    None
                }
            },
            Err(e) => {
                error!("Error while trying to get bookmarks room {e}");
                None
            }
            _ => None,
        }
    }

    /// Remove an event from the list of bookmarks
    /// It takes the [`EventId`] of the Bookmark event
    /// sent in the bookmarks room.
    pub async fn unbookmark_event(
        &self,
        bookmark_event_id: &EventId,
    ) -> crate::Result<redact_event::v3::Response> {
        let redact_closure = move |r: Room| async move {
            r.redact(bookmark_event_id, Some("Bookmark removal"), None).await
        };
        let maybe_result = self.with_bookmarks_room(redact_closure, true).await;
        maybe_result
            .ok_or(crate::Error::BookmarksError)?
            .await
            .map_err(|e| crate::Error::Http(Box::new(e)))
    }

    /// Checks whether an event is bookmarked or not from its original
    /// event_id and returns its associated bookmark_event_id if its
    /// the case.
    pub async fn is_event_bookmarked(
        &self,
        original_target_event_id: &EventId,
    ) -> Option<OwnedEventId> {
        self.bookmark_index()
            .lock()
            .await
            .get_bookmark_id_for_event(original_target_event_id)
            .map(|b| b.bookmark_id())
    }
}

async fn create_bookmarks_room(client: &Client) -> crate::Result<Room> {
    let mut request = create_room::v3::Request::new();
    request.visibility = ruma::api::client::room::Visibility::Private;
    request.name = Some("Bookmarks".to_owned());
    // TODO: add a topic with a sort of explanation for fallback views ?
    // request.topic = Some("This room has been created by your client: {name of client} to store bookmarks.")
    let mut creation_content = CreationContent::new();
    creation_content.room_type = Some(ruma::room::RoomType::Bookmarks);
    request.creation_content = Some(Raw::new(&creation_content).unwrap());

    let room = client.create_room(request).await?;

    client
        .account()
        .set_account_data(BookmarksRoomEventContent::new(room.room_id().to_owned()))
        .await?;

    #[cfg(feature = "e2e-encryption")]
    {
        room.enable_encryption().await?;
    }

    Ok(room)
}

#[derive(Debug)]
struct GlobalBookmarkSearchRoomState {
    /// The room for which we're storing state.
    room: Room,

    /// The current start offset in the search results for this room, or `None`
    /// if we haven't called the iterator for this room yet.
    offset: Option<usize>,
}

impl GlobalBookmarkSearchRoomState {
    fn new(room: Room) -> Self {
        Self { room, offset: None }
    }
}

/// A builder for a [`GlobalSearchIterator`] that allows to configure the
/// initial working set of rooms to search in.
#[derive(Debug)]
pub struct GlobalBookmarkSearchBuilder {
    client: Client,

    /// The search query, directly forwarded to the search API.
    query: String,

    /// Number of results to return (at most) per batch when calling
    /// [`GlobalSearchIterator::next()`].
    num_results_per_batch: usize,

    /// The working set of rooms to search in.
    room_set: Vec<Room>,
}

impl GlobalBookmarkSearchBuilder {
    /// Create a new global search on all the joined rooms.
    fn new(client: Client, query: String, num_results_per_batch: usize) -> Self {
        let room_set = client.rooms_filtered(RoomStateFilter::JOINED);
        Self { client, query, room_set, num_results_per_batch }
    }

    /// Keep only the DM rooms from the initial working set.
    pub async fn only_dm_rooms(mut self) -> Result<Self, crate::Error> {
        let mut to_remove = HashSet::new();
        for room in &self.room_set {
            if !room.compute_is_dm().await? {
                to_remove.insert(room.room_id().to_owned());
            }
        }
        self.room_set.retain(|room| !to_remove.contains(room.room_id()));
        Ok(self)
    }

    /// Keep only non-DM rooms (groups) from the initial working set.
    pub async fn no_dms(mut self) -> Result<Self, crate::Error> {
        let mut to_remove = HashSet::new();
        for room in &self.room_set {
            if room.compute_is_dm().await? {
                to_remove.insert(room.room_id().to_owned());
            }
        }
        self.room_set.retain(|room| !to_remove.contains(room.room_id()));
        Ok(self)
    }

    /// Build the [`GlobalSearchIterator`] from this builder.
    pub fn build(self) -> GlobalBookmarkSearchIterator {
        GlobalBookmarkSearchIterator {
            client: self.client,
            query: self.query,
            room_state: Vec::from_iter(
                self.room_set.into_iter().map(GlobalBookmarkSearchRoomState::new),
            ),
            current_batch: Vec::new(),
            num_results_per_batch: self.num_results_per_batch,
        }
    }
}

/// An async iterator for a search query across multiple rooms.
#[derive(Debug)]
pub struct GlobalBookmarkSearchIterator {
    client: Client,

    /// The search query, directly forwarded to the search API.
    query: String,

    /// The state for each room in the working list, that may still have
    /// results.
    ///
    /// This list is bound to shrink as we exhaust search results for each room,
    /// until it's empty and the overall iteration is done.
    room_state: Vec<GlobalBookmarkSearchRoomState>,

    /// A buffer for the current batch of results across all rooms, sorted by
    /// score descending so results are returned in relevance order.
    current_batch: Vec<(f32, OwnedRoomId, IndexedBookmark)>,

    /// Number of results to return (at most) per batch when calling
    /// [`Self::next()`].
    num_results_per_batch: usize,
}

impl GlobalBookmarkSearchIterator {
    /// Return the next batch of event IDs matching the search query across all
    /// rooms, or `None` if there are no more results.
    pub async fn next(
        &mut self,
    ) -> Result<Option<Vec<(OwnedRoomId, IndexedBookmark)>>, SearchError> {
        if self.room_state.is_empty() {
            return Ok(None);
        }

        // If there was enough results from a previous room iteration, return them
        // immediately (they're already sorted from the previous fill).
        if self.current_batch.len() >= self.num_results_per_batch {
            return Ok(Some(
                self.current_batch
                    .drain(0..self.num_results_per_batch)
                    .map(|(_, room_id, event_id)| (room_id, event_id))
                    .collect(),
            ));
        }

        let mut to_remove = HashSet::new();

        // Search across all non-done rooms for `num_results`, and accumulate them in
        // `Self::current_batch`.
        for room_state in &mut self.room_state {
            let room_results = room_state
                .room
                .search_room_bookmarks(&self.query, self.num_results_per_batch, room_state.offset)
                .await?;

            if room_results.is_empty() {
                // We've exhausted results for this room, mark it for removal.
                to_remove.insert(room_state.room.room_id().to_owned());
            } else {
                // Move the start offset for the room forward.
                room_state.offset = Some(room_state.offset.unwrap_or(0) + room_results.len());

                // Append the search results to the current batch.
                self.current_batch.extend(room_results.into_iter().map(|indexed_bookmark| {
                    (indexed_bookmark.score, room_state.room.room_id().to_owned(), indexed_bookmark)
                }));

                if self.current_batch.len() >= self.num_results_per_batch {
                    // We have enough events to return now.
                    break;
                }
            }
        }

        // Delete rooms for which we've exhausted search results from the working list.
        for room_id in to_remove {
            self.room_state.retain(|room_state| room_state.room.room_id() != room_id);
        }

        if !self.current_batch.is_empty() {
            // Sort by score descending so cross-room results are returned in relevance
            // order.
            self.current_batch.sort_unstable_by(|a, b| b.0.total_cmp(&a.0));
            let high = self.num_results_per_batch.min(self.current_batch.len());
            Ok(Some(
                self.current_batch
                    .drain(0..high)
                    .map(|(_, room_id, indexed_bookmark)| (room_id, indexed_bookmark))
                    .collect(),
            ))
        } else {
            debug_assert!(self.room_state.is_empty());
            Ok(None)
        }
    }

    /// Returns [`TimelineEvent`]s instead of event IDs, by loading the events
    /// from the store or from network.
    pub async fn next_events(
        &mut self,
    ) -> Result<Option<Vec<(OwnedRoomId, TimelineEvent)>>, SearchError> {
        let Some(bookmarks) = self.next().await? else {
            return Ok(None);
        };
        let mut results = Vec::with_capacity(bookmarks.len());
        for (room_id, bookmark) in bookmarks {
            let Some(room) = self.client.get_room(&room_id) else {
                continue;
            };
            let (original_event, mut replacements) = room
                .load_or_fetch_event_with_relations(
                    &bookmark.target_event_id,
                    Some(vec![RelationType::Replacement]),
                    None,
                )
                .await?;

            results.push((room_id, replacements.pop().unwrap_or(original_event)));
        }
        Ok(Some(results))
    }
}
