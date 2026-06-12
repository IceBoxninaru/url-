from __future__ import annotations

from jobs.models import CaptureJob
from resources.models import Resource, ResourceStatus
from snapshots.models import FetchMethod, Snapshot

from .types import (
    AIResult,
    CapturedImage,
    CapturedVideo,
    CaptureResult,
    DownloadedMediaCandidate,
    DownloadedVideoAssets,
    LinkCheckResult,
    MediaProbeResult,
)
from .storage import (
    build_resource_directory,
    build_storage_asset_path,
    filter_existing_snapshot_assets,
    get_capture_files,
    get_snapshot_screenshot_file,
    move_storage_file,
    resolve_asset_file_path,
    resolve_storage_file_path,
    write_storage_file,
)
from .snapshots import build_snapshot_diff_context, build_snapshot_diff_items, get_previous_snapshot
from .urls import normalize_url
from .link_checks import (
    DELETE_MARKERS,
    check_resource_link_status,
    detect_deleted_like,
    perform_link_check,
    should_refresh_link_check,
)
from .jobs import enqueue_ai_job, enqueue_capture_job, status_from_snapshot
from .translation import (
    GoogleTranslateProvider,
    TRANSLATION_ENDPOINT,
    TRANSLATION_MAX_CHUNK_CHARS,
    TRANSLATION_MAX_SOURCE_CHARS,
    TranslationProvider,
    build_translation_source_text,
    get_translation_provider,
    is_probably_japanese_text,
    normalize_ai_text,
    split_translation_chunks,
    translate_text_chunk_to_japanese,
    translate_text_to_japanese,
)
from .ai_heuristics import STOP_WORDS, infer_category, similar_resource_ids, suggest_tags
from .local_llm import (
    extract_local_llm_content,
    local_llm_chat,
    local_llm_chat_completion_url,
    local_llm_models,
    normalize_local_llm_tags,
    parse_local_llm_json,
    truncate_ai_output,
)
from .artifacts import delete_resource_with_artifacts
from .content import extract_metadata, extract_text_from_html
from .media_discovery import (
    AUDIO_EXTENSIONS,
    ENCODED_MEDIA_URL_PATTERN,
    MEDIA_DISCOVERY_STRATEGIES,
    MEDIA_TEXT_SCAN_MAX_CHARS,
    MediaDiscoveryStrategy,
    RAW_MEDIA_URL_PATTERN,
    VIDEO_EXTENSIONS,
    build_media_candidate,
    classify_media_candidate_kind,
    collect_image_urls,
    collect_playwright_image_urls,
    collect_playwright_media_candidates,
    collect_playwright_video_urls,
    collect_video_candidate_details,
    collect_video_urls,
    decode_media_text_url,
    dedupe_urls,
    explain_media_candidate_skip,
    extract_media_candidate_urls_from_text,
    filter_image_candidate_urls,
    filter_video_candidate_urls,
    get_playwright_media_scope,
    get_playwright_profile_path,
    get_playwright_storage_state_path,
    guess_image_extension,
    guess_video_extension,
    html_unescape_and_clean_url,
    is_instagram_domain,
    is_observed_media_request,
    is_observed_media_response,
    is_observed_video_response,
    is_probable_audio_url,
    is_probable_media_url,
    is_probable_video_url,
    is_relevant_image_candidate,
    is_relevant_video_candidate,
    is_scoped_social_capture_domain,
    is_x_domain,
    is_x_hls_playlist_url,
    is_x_master_playlist_url,
    is_x_progressive_video_url,
    matches_configured_domain,
    merge_media_candidates,
    normalize_media_candidate_url,
    resolve_storage_state_path,
    score_instagram_video_candidate,
    score_video_candidate,
    score_x_video_candidate,
    select_media_discovery_strategy,
    should_scan_media_response_body,
    should_skip_image_url,
    should_skip_video_url,
    supports_video_capture,
)
from .ai_pipeline import (
    LOCAL_LLM_CATEGORIES,
    LOCAL_LLM_PROVIDERS,
    build_ai_source_text,
    build_local_llm_messages,
    run_ai_pipeline,
    run_local_llm_pipeline,
)

from .media_downloads import (
    MediaProbe,
    append_instagram_skip_log,
    build_instagram_audio_exploration_candidates,
    build_instagram_media_candidates,
    cleanup_capture_result,
    create_temp_download_path,
    delete_temp_file,
    download_image_assets,
    download_instagram_candidate,
    download_instagram_candidate_without_probe,
    download_instagram_video_assets,
    download_video_assets,
    durations_match,
    get_ffmpeg_executable,
    get_ffprobe_executable,
    merge_instagram_streams,
    pick_instagram_merge_pair,
    remux_x_hls_to_mp4,
    score_downloaded_media_candidate,
    summarize_instagram_video_result,
    validate_downloaded_video_file,
)
from .capture import (
    choose_capture_result,
    fetch_with_http,
    fetch_with_playwright,
    should_use_playwright,
)
from .persistence import next_snapshot_no, persist_snapshot
from .job_handlers import execute_ai_job, execute_capture_job
