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

use matrix_sdk_base::deserialized_responses::TimelineEvent;
#[cfg(doc)]
use matrix_sdk_search::bookmarks::BookmarkIndex;
pub use matrix_sdk_search::{bookmarks::IndexedBookmark, error::IndexError};
use ruma::{
    EventId, OwnedEventId, RoomId,
    api::client::{
        redact::redact_event,
        room::create_room::{self, v3::CreationContent},
    },
    events::{
        bookmark::{BookmarkEventContent, PointerContentBlock},
        bookmarks_room::BookmarksRoomEventContent,
    },
    serde::Raw,
};
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
        self.client
            .search_bookmarks(query, max_number_of_results, pagination_offset, Some(self.room_id()))
            .await
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
    pub async fn bookmarked_event_ids(
        &self,
    ) -> Result<std::collections::HashSet<OwnedEventId>, IndexError> {
        // Number of bookmarks to load per index query.
        const BATCH_SIZE: usize = 100;

        let mut event_ids = std::collections::HashSet::new();
        let mut offset = 0;

        loop {
            let batch = self.search_room_bookmarks("*", BATCH_SIZE, Some(offset)).await?;
            if batch.is_empty() {
                break;
            }
            offset += batch.len();
            event_ids.extend(batch.into_iter().map(|bookmark| bookmark.original_event_id));
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
            results.push(self.room.load_or_fetch_event(&bookmark.event_id, None).await?);
        }
        Ok(Some(results))
    }
}

impl Client {
    /// Search across the global bookmarks index with the given query.
    pub async fn search_bookmarks(
        &self,
        query: &str,
        max_number_of_results: usize,
        pagination_offset: Option<usize>,
        room_id_filter: Option<&RoomId>,
    ) -> Result<Vec<IndexedBookmark>, IndexError> {
        let mut index = self.bookmark_index().lock().await;
        index.search(query, max_number_of_results, pagination_offset, room_id_filter)
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
