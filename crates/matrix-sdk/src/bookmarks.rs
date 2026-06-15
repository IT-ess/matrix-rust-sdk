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
    EventId, RoomId,
    api::client::{
        redact::redact_event,
        room::create_room::{self, v3::CreationContent},
    },
    events::bookmark::{BookmarkEventContent, PointerContentBlock},
    serde::Raw,
};

use crate::{Client, Room, message_search::SearchError, room::futures::SendMessageLikeEventResult};

impl Room {
    /// Get bookmarked events of this room and return at most
    /// max_number_of_results results.
    pub async fn get_room_bookmarks(
        &self,
        max_number_of_results: usize,
        pagination_offset: Option<usize>,
    ) -> Result<Vec<IndexedBookmark>, IndexError> {
        self.search_room_bookmarks("*", max_number_of_results, pagination_offset).await
    }

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

    /// Remove an event from the list of bookmarks
    pub async fn unbookmark_event(
        &self,
        bookmark_event_id: &EventId,
    ) -> crate::Result<redact_event::v3::Response> {
        let redact_closure = move |r: Room| async move {
            r.redact(bookmark_event_id, Some("Bookmark removal"), None).await
        };
        let maybe_result = self.client().with_bookmarks_room(redact_closure, true).await;
        maybe_result
            .ok_or(crate::Error::BookmarksError)?
            .await
            .map_err(|e| crate::Error::Http(Box::new(e)))
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
    pub fn get_bookmarks_room(&self) -> Option<Room> {
        let all_rooms = self.rooms();
        // TODO: manage the case where multiple bookmark rooms
        // exist (compare room versions ?)
        all_rooms.into_iter().find(|r| r.is_bookmarks())
    }

    /// Do an operation with the bookmarks room
    pub async fn with_bookmarks_room<F, R>(&self, f: F, create_if_needed: bool) -> Option<R>
    where
        F: FnOnce(Room) -> R,
    {
        if let Some(room) = self.get_bookmarks_room() {
            Some(f(room))
        } else if create_if_needed && let Some(room) = create_bookmarks_room(self).await.ok() {
            Some(f(room))
        } else {
            None
        }
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

    #[cfg(feature = "e2e-encryption")]
    {
        room.enable_encryption().await?;
    }

    Ok(room)
}
