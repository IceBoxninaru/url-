from __future__ import annotations

from snapshots.models import Snapshot


def get_previous_snapshot(snapshot: Snapshot | None) -> Snapshot | None:
    if snapshot is None:
        return None
    return (
        Snapshot.objects.filter(resource=snapshot.resource, snapshot_no__lt=snapshot.snapshot_no)
        .order_by("-snapshot_no")
        .first()
    )


def build_snapshot_diff_items(snapshot: Snapshot | None, previous_snapshot: Snapshot | None) -> list[dict]:
    if snapshot is None or previous_snapshot is None:
        return []

    diff_items: list[dict] = []

    def add_diff(label: str, before, after) -> None:
        if before == after:
            return
        diff_items.append(
            {
                "label": label,
                "before": before if before not in {None, ""} else "-",
                "after": after if after not in {None, ""} else "-",
            }
        )

    add_diff("ページタイトル", previous_snapshot.page_title, snapshot.page_title)
    add_diff("HTTP", previous_snapshot.http_status, snapshot.http_status)
    add_diff("取得方法", previous_snapshot.get_fetch_method_display(), snapshot.get_fetch_method_display())
    add_diff("保存画像数", f"{previous_snapshot.image_count} 件", f"{snapshot.image_count} 件")
    add_diff("保存動画数", f"{previous_snapshot.video_count} 件", f"{snapshot.video_count} 件")

    previous_text = (previous_snapshot.extracted_text or "").strip()
    current_text = (snapshot.extracted_text or "").strip()
    if previous_text != current_text:
        diff_items.append(
            {
                "label": "本文",
                "before": f"{len(previous_text)} 文字",
                "after": f"{len(current_text)} 文字",
            }
        )

    add_diff("エラー", previous_snapshot.error_message, snapshot.error_message)
    return diff_items


def build_snapshot_diff_context(snapshot: Snapshot | None) -> dict:
    previous_snapshot = get_previous_snapshot(snapshot)
    return {
        "previous_snapshot": previous_snapshot,
        "items": build_snapshot_diff_items(snapshot, previous_snapshot),
    }
