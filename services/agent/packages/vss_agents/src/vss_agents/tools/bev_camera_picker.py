# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Pick the camera that best shows a fused (BEV) incident.

Incidents from a multi-camera fusion sensor such as ``bev-sensor-1`` have no
video stream of their own. The per-camera detections (``mdx-raw``) carry the
same global object IDs, so for each camera we score how well *all* incident
objects are seen during the incident window and return the best camera.

Score per frame = min over incident objects of (bbox area x visibility), with a
missing object counting as 0, so a camera only scores if every party is in
view. The camera score is the mean over its frames.
"""

import asyncio
from collections import defaultdict
from datetime import UTC
from datetime import datetime
from datetime import timedelta
import logging
from typing import Any

from elasticsearch import AsyncElasticsearch

logger = logging.getLogger(__name__)


def _shift_iso(timestamp: str, seconds: float) -> str:
    """Shift an ISO-8601 UTC timestamp (``...Z``) by ``seconds``; ES date math has no millisecond unit."""
    moment = datetime.fromisoformat(timestamp.replace("Z", "+00:00")) + timedelta(seconds=seconds)
    return moment.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%S.") + f"{moment.microsecond // 1000:03d}Z"


def _object_score(obj: dict[str, Any]) -> float:
    bbox = obj.get("bbox") or {}
    width = max(0.0, float(bbox.get("rightX", 0)) - float(bbox.get("leftX", 0)))
    height = max(0.0, float(bbox.get("bottomY", 0)) - float(bbox.get("topY", 0)))
    visibility = float((obj.get("info") or {}).get("visibility") or 0)
    return width * height * visibility


def score_cameras(hits: list[dict[str, Any]], object_ids: list[str], primary_id: str | None = None) -> dict[str, float]:
    """Mean per-frame score for each camera from ES hits with ``objects`` inner_hits.

    With ``primary_id`` (the incident's center object, e.g. the robot) a frame
    scores min(primary, best of the others), so an incident with several people
    still gets a camera that shows the robot and at least one person. Without
    it, every object must be in view (min over all).
    """
    per_camera: dict[str, list[float]] = defaultdict(list)
    for hit in hits:
        camera = (hit.get("_source") or {}).get("sensorId")
        if not camera:
            continue
        best: dict[str, float] = {}
        for inner in hit.get("inner_hits", {}).get("objects", {}).get("hits", {}).get("hits", []):
            obj = inner.get("_source") or {}
            obj_id = str(obj.get("id"))
            best[obj_id] = max(best.get(obj_id, 0.0), _object_score(obj))
        others = [obj_id for obj_id in object_ids if obj_id != primary_id]
        if primary_id in object_ids and others:
            frame_score = min(best.get(primary_id, 0.0), max(best.get(obj_id, 0.0) for obj_id in others))
        else:
            frame_score = min(best.get(obj_id, 0.0) for obj_id in object_ids)
        per_camera[camera].append(frame_score)
    return {camera: sum(scores) / len(scores) for camera, scores in per_camera.items() if scores}


class BevCameraPicker:
    """Chooses a camera for a fused-sensor incident from per-camera detections."""

    def __init__(
        self, es_url: str, raw_index: str = "mdx-raw-*", max_frames: int = 2000, max_parallel_media: int = 3
    ) -> None:
        self._es = AsyncElasticsearch(es_url)
        # Callers hold this while fetching VST media for a picked camera.
        self.media_slots = asyncio.Semaphore(max_parallel_media)
        self._raw_index = raw_index
        self._max_frames = max_frames

    async def pick(
        self, object_ids: list[str], start_time: str, end_time: str, primary_id: str | None = None
    ) -> str | None:
        """Return the best camera name, or None when no camera sees every object."""
        object_ids = [str(obj_id) for obj_id in object_ids if obj_id is not None]
        if not object_ids or not start_time or not end_time:
            return None
        query = {
            "bool": {
                "filter": [
                    {"range": {"timestamp": {"gte": start_time, "lte": end_time}}},
                    {
                        "nested": {
                            "path": "objects",
                            "query": {"terms": {"objects.id.keyword": object_ids}},
                            "inner_hits": {
                                "size": len(object_ids),
                                "_source": ["objects.id", "objects.bbox", "objects.info.visibility"],
                            },
                        }
                    },
                ]
            }
        }
        response = await self._es.search(index=self._raw_index, query=query, size=self._max_frames, source=["sensorId"])
        scores = score_cameras(response["hits"]["hits"], object_ids, str(primary_id) if primary_id else None)
        if not scores:
            return None
        camera = max(scores, key=lambda name: scores[name])
        logger.info(f"Camera scores for objects {object_ids}: {scores} -> {camera}")
        return camera if scores[camera] > 0 else None

    async def object_types(
        self, object_ids: list[str], start_time: str, end_time: str, behavior_index: str = "mdx-behavior-*"
    ) -> dict[str, str]:
        """Map object ID -> object type from the latest behavior that started before the incident ended.

        Behaviors are written when a behavior segment closes, so an incident that is
        still recent may have no overlapping behavior yet; the object's previous
        segment (same tracking ID) carries the same type.
        """
        object_ids = [str(obj_id) for obj_id in object_ids if obj_id is not None]
        if not object_ids or not start_time or not end_time:
            return {}
        query = {
            "bool": {
                "filter": [
                    {"terms": {"object.id.keyword": object_ids}},
                    {"range": {"timestamp": {"lte": end_time}}},
                ]
            }
        }
        aggs = {
            "ids": {
                "terms": {"field": "object.id.keyword", "size": len(object_ids)},
                "aggs": {
                    "latest": {"top_hits": {"size": 1, "sort": [{"timestamp": "desc"}], "_source": ["object.type"]}}
                },
            }
        }
        response = await self._es.search(index=behavior_index, query=query, aggs=aggs, size=0)
        types: dict[str, str] = {}
        for bucket in response.get("aggregations", {}).get("ids", {}).get("buckets", []):
            hits = bucket.get("latest", {}).get("hits", {}).get("hits", [])
            obj_type = ((hits[0].get("_source") or {}).get("object") or {}).get("type") if hits else None
            if obj_type:
                types[str(bucket["key"])] = str(obj_type)
        return types

    async def person_aliases(
        self,
        windows: list[tuple[str, str, list[str]]],
        bev_index: str = "mdx-bev-*",
        max_distance_m: float = 0.5,
        min_common_frames: int = 3,
        handoff_distance_m: float = 0.8,
        handoff_lookback_s: float = 1.0,
        max_windows: int = 100,
        max_parallel: int = 4,
    ) -> dict[str, str]:
        """Find Person IDs that are duplicates of one physical person.

        Tracking can split one person into several IDs (e.g. at a looped video's
        seam); the copies then move together. For each (start, end, object_ids)
        window, two Person IDs whose median BEV distance is below
        ``max_distance_m`` over at least ``min_common_frames`` shared frames are
        merged. A Person ID whose first appearance is within ``handoff_distance_m``
        of an older Person ID seen in the preceding ``handoff_lookback_s`` is a
        hand-off of the same person (the old track ended, a new ID took over) and
        is merged too. Returns alias ID -> canonical ID (the lowest numeric ID,
        i.e. the earliest track), across all windows.
        """
        parent: dict[str, str] = {}

        def find(node: str) -> str:
            while parent.setdefault(node, node) != node:
                parent[node] = parent[parent[node]]
                node = parent[node]
            return node

        def sort_key(node: str) -> tuple[int, int | str]:
            return (0, int(node)) if node.isdigit() else (1, node)

        def union(a: str, b: str) -> None:
            root_a, root_b = find(a), find(b)
            if root_a != root_b:
                keep, drop = sorted((root_a, root_b), key=sort_key)
                parent[drop] = keep

        slots = asyncio.Semaphore(max_parallel)

        async def scan(start: str, end: str, ids: list[str]) -> list[tuple[str, str]]:
            async with slots:
                response = await self._es.search(
                    index=bev_index,
                    query={"range": {"timestamp": {"gte": start, "lte": end}}},
                    size=self._max_frames,
                    source=["objects.id", "objects.type", "objects.bbox3d.coordinates"],
                )
            wanted = set(ids)
            distances: dict[tuple[str, str], list[float]] = defaultdict(list)
            for hit in response["hits"]["hits"]:
                people = {}
                for obj in (hit.get("_source") or {}).get("objects", []):
                    coords = (obj.get("bbox3d") or {}).get("coordinates") or []
                    obj_id = str(obj.get("id"))
                    if obj.get("type") == "Person" and obj_id in wanted and len(coords) >= 2:
                        people[obj_id] = (float(coords[0]), float(coords[1]))
                found = sorted(people)
                for i, a in enumerate(found):
                    for b in found[i + 1 :]:
                        (ax, ay), (bx, by) = people[a], people[b]
                        distances[(a, b)].append(((ax - bx) ** 2 + (ay - by) ** 2) ** 0.5)
            pairs = []
            for pair, values in distances.items():
                if len(values) >= min_common_frames:
                    values.sort()
                    if values[len(values) // 2] < max_distance_m:
                        pairs.append(pair)
            return pairs

        async def handoff(person_id: str) -> tuple[str, str] | None:
            async with slots:
                first = await self._es.search(
                    index=bev_index,
                    query={
                        "nested": {
                            "path": "objects",
                            "query": {
                                "bool": {
                                    "filter": [
                                        {"term": {"objects.id.keyword": person_id}},
                                        {"term": {"objects.type.keyword": "Person"}},
                                    ]
                                }
                            },
                            "inner_hits": {"size": 1, "_source": ["objects.bbox3d.coordinates"]},
                        }
                    },
                    sort=[{"timestamp": "asc"}],
                    size=1,
                    source=["timestamp"],
                )
                hits = first["hits"]["hits"]
                if not hits:
                    return None
                born = hits[0]["_source"]["timestamp"]
                inner = hits[0]["inner_hits"]["objects"]["hits"]["hits"]
                coords = ((inner[0].get("_source") or {}).get("bbox3d") or {}).get("coordinates") if inner else None
                if not coords or len(coords) < 2:
                    return None
                before = await self._es.search(
                    index=bev_index,
                    query={"range": {"timestamp": {"gte": _shift_iso(born, -handoff_lookback_s), "lt": born}}},
                    sort=[{"timestamp": "desc"}],
                    size=int(handoff_lookback_s * 40),
                    source=["objects.id", "objects.type", "objects.bbox3d.coordinates"],
                )
            best: tuple[float, str] | None = None
            for hit in before["hits"]["hits"]:
                for obj in (hit.get("_source") or {}).get("objects", []):
                    other = str(obj.get("id"))
                    pos = (obj.get("bbox3d") or {}).get("coordinates") or []
                    if obj.get("type") != "Person" or other == person_id or len(pos) < 2:
                        continue
                    dist = ((float(pos[0]) - float(coords[0])) ** 2 + (float(pos[1]) - float(coords[1])) ** 2) ** 0.5
                    if dist < handoff_distance_m and (best is None or dist < best[0]):
                        best = (dist, other)
            return (best[1], person_id) if best else None

        candidates = [(s, e, [str(i) for i in ids]) for s, e, ids in windows if s and e and len(ids) >= 2]
        results = await asyncio.gather(
            *(scan(s, e, ids) for s, e, ids in candidates[:max_windows] if len(ids) >= 3), return_exceptions=True
        )
        for result in results:
            if isinstance(result, BaseException):
                logger.warning(f"Person alias scan failed: {result!r}")
                continue
            for a, b in result:
                union(a, b)

        # Hand-offs: only for IDs that are not already merged and not the oldest.
        all_ids = sorted({i for _, _, ids in candidates for i in ids}, key=sort_key)
        pending = [i for i in all_ids[1:] if find(i) == i][:max_windows]
        handoffs = await asyncio.gather(*(handoff(i) for i in pending), return_exceptions=True)
        for result in handoffs:
            if isinstance(result, BaseException):
                logger.warning(f"Person hand-off scan failed: {result!r}")
            elif result:
                union(*result)
        aliases = {node: find(node) for node in list(parent) if find(node) != node}
        if aliases:
            logger.info(f"Merged duplicate Person IDs: {aliases}")
        return aliases

    async def close(self) -> None:
        await self._es.close()
