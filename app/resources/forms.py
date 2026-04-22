from __future__ import annotations

import re

from django import forms
from django.db import transaction

from resources.models import Resource, ResourceStatus, ReviewState, SaveReason
from resources.services import normalize_url
from tags.models import Tag

NOTE_TEMPLATE_CHOICES = list(SaveReason.choices)

NEW_TAG_SPLIT_RE = re.compile(r"[\n,、]+")


def parse_new_tag_names(raw_value: str) -> list[str]:
    if not raw_value:
        return []

    max_length = Tag._meta.get_field("name").max_length
    normalized_names: list[str] = []
    seen_names: set[str] = set()
    for candidate in NEW_TAG_SPLIT_RE.split(raw_value):
        name = candidate.strip()
        if not name:
            continue
        if len(name) > max_length:
            raise forms.ValidationError(f"タグ名は {max_length} 文字以内で入力してください。")
        key = name.casefold()
        if key not in seen_names:
            seen_names.add(key)
            normalized_names.append(name)
    return normalized_names


class ResourceForm(forms.ModelForm):
    save_reason = forms.ChoiceField(
        required=False,
        choices=[("", "選択なし")] + NOTE_TEMPLATE_CHOICES,
        label="保存理由",
    )
    next_action = forms.CharField(
        required=False,
        label="次の行動",
        widget=forms.TextInput(attrs={"placeholder": "例: 実装で試す / 比較して決める / 共有する"}),
    )
    recheck_at = forms.DateField(
        required=False,
        label="再確認日",
        widget=forms.DateInput(attrs={"type": "date"}),
    )
    original_url = forms.CharField(
        max_length=2000,
        label="URL",
        widget=forms.URLInput(attrs={"placeholder": "https://example.com/article"}),
    )
    tags = forms.ModelMultipleChoiceField(
        queryset=Tag.objects.none(),
        required=False,
        label="タグ",
        widget=forms.CheckboxSelectMultiple,
    )
    new_tags = forms.CharField(
        required=False,
        label="新しいタグを追加",
        widget=forms.Textarea(
            attrs={
                "rows": 3,
                "placeholder": "1行に1つずつ入力\n例:\n翻訳待ち\n授業メモ",
            }
        ),
        help_text="1行に1つずつ入力すると、複数のタグを追加できます。追加したタグは今後も使えます。",
    )

    class Meta:
        model = Resource
        fields = [
            "original_url",
            "title_manual",
            "save_reason",
            "next_action",
            "recheck_at",
            "note",
            "favorite",
            "search_only",
            "capture_images",
            "capture_videos",
            "review_state",
        ]
        labels = {
            "title_manual": "手動タイトル",
            "note": "メモ",
            "favorite": "お気に入り",
            "search_only": "一覧には出さず、検索時のみ表示する",
            "capture_images": "画像を保存する",
            "capture_videos": "動画を保存する",
            "review_state": "見直し状態",
        }
        widgets = {
            "title_manual": forms.TextInput(attrs={"placeholder": "一覧で目立たせたいタイトル"}),
            "note": forms.Textarea(attrs={"rows": 4, "placeholder": "内容や文脈をメモ"}),
        }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.existing_resource: Resource | None = None
        self.fields["tags"].queryset = Tag.objects.all().order_by("sort_order", "name")
        self.fields["favorite"].label = "お気に入りにする"
        self.fields["search_only"].help_text = "オンにすると通常の一覧では隠れ、検索入力時だけ表示されます。"
        self.fields["capture_images"].help_text = "オフにすると画像ファイルを保存しません。"
        self.fields["capture_videos"].help_text = "オフにすると動画ファイルを保存しません。"
        self.fields["review_state"].label = "見直し状態"
        self.fields["review_state"].required = False
        self.fields["review_state"].initial = ReviewState.NONE
        self.fields["save_reason"].help_text = "何のために保存したかを整理して持てます。"
        self.fields["next_action"].help_text = "次に何をするURLかを短く書いておけます。"
        self.fields["recheck_at"].help_text = "あとで見返したい日を設定できます。"
        self.order_fields(
            [
                "original_url",
                "title_manual",
                "save_reason",
                "next_action",
                "recheck_at",
                "note",
                "review_state",
                "favorite",
                "search_only",
                "capture_images",
                "capture_videos",
                "tags",
                "new_tags",
            ]
        )
        if self.instance.pk:
            self.initial["tags"] = self.instance.tags.all()
            if self.instance.save_reason:
                self.initial["save_reason"] = self.instance.save_reason
            for template_value, _ in NOTE_TEMPLATE_CHOICES:
                note = (self.instance.note or "").strip()
                if not self.initial.get("save_reason") and (note == template_value or note.startswith(f"{template_value}\n")):
                    self.initial["save_reason"] = template_value
                    break

    def clean_original_url(self):
        original_url = self.cleaned_data["original_url"].strip()
        try:
            self.cleaned_normalized_url = normalize_url(original_url)
        except ValueError as exc:
            raise forms.ValidationError(str(exc)) from exc

        duplicate_queryset = Resource.objects.filter(normalized_url=self.cleaned_normalized_url)
        if self.instance.pk:
            duplicate_queryset = duplicate_queryset.exclude(pk=self.instance.pk)
        self.existing_resource = duplicate_queryset.select_related("latest_snapshot").first()
        if self.existing_resource is not None:
            raise forms.ValidationError("このURLは登録済みです。")

        return original_url

    def clean_new_tags(self) -> list[str]:
        raw_value = self.cleaned_data.get("new_tags", "")
        return parse_new_tag_names(raw_value)

    def resolve_tags(self) -> list[Tag]:
        resolved_tags = list(self.cleaned_data.get("tags") or [])
        existing_names = {tag.name.casefold(): tag for tag in resolved_tags}
        for name in self.cleaned_data.get("new_tags") or []:
            key = name.casefold()
            if key in existing_names:
                continue
            tag = Tag.objects.filter(name__iexact=name).first()
            if tag is None:
                tag = Tag.objects.create(name=name)
            existing_names[key] = tag
            resolved_tags.append(tag)
        return resolved_tags

    def save(self, commit=True):
        resource = super().save(commit=False)
        resource.normalized_url = self.cleaned_normalized_url
        resource.update_domain_from_url()
        resource.review_state = self.cleaned_data.get("review_state") or ReviewState.NONE
        resource.save_reason = (self.cleaned_data.get("save_reason") or "").strip()
        resource.next_action = (self.cleaned_data.get("next_action") or "").strip()
        resource.recheck_at = self.cleaned_data.get("recheck_at")
        resource.note = (resource.note or "").strip()
        resolved_tags = self.resolve_tags()
        if commit:
            resource.save()
            resource.tags.set(resolved_tags)
        else:
            self._pending_tags = resolved_tags
        return resource

    def save_m2m(self):
        if hasattr(self, "_pending_tags"):
            self.instance.tags.set(self._pending_tags)


class ResourceFilterForm(forms.Form):
    q = forms.CharField(
        required=False,
        label="検索",
        widget=forms.TextInput(
            attrs={
                "placeholder": "URL / タイトル / 本文 / メモを検索",
            }
        ),
    )
    domain = forms.ChoiceField(
        required=False,
        choices=[("", "すべて")],
        label="ドメイン",
    )
    tags = forms.ModelMultipleChoiceField(
        queryset=Tag.objects.none(),
        required=False,
        label="タグ",
        widget=forms.CheckboxSelectMultiple,
    )
    favorite_only = forms.BooleanField(required=False, label="お気に入りのみ")
    status = forms.ChoiceField(
        required=False,
        choices=[("", "すべて")] + list(ResourceStatus.choices),
        label="取得状態",
    )
    review_state = forms.ChoiceField(
        required=False,
        choices=[("", "すべて")] + list(ReviewState.choices),
        label="見直し",
    )
    save_reason = forms.ChoiceField(
        required=False,
        choices=[("", "すべて")] + NOTE_TEMPLATE_CHOICES,
        label="保存理由",
    )
    recheck_due_only = forms.BooleanField(required=False, label="再確認待ちのみ")

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.fields["tags"].queryset = Tag.objects.all().order_by("sort_order", "name")
        domains = (
            Resource.objects.exclude(domain="")
            .order_by("domain")
            .values_list("domain", flat=True)
            .distinct()
        )
        self.fields["domain"].choices = [("", "すべて")] + [(domain, domain) for domain in domains]


class ResourceBulkEditForm(forms.Form):
    review_state = forms.ChoiceField(
        required=False,
        choices=[("", "変更しない")] + list(ReviewState.choices),
        label="見直し状態",
    )
    save_reason = forms.ChoiceField(
        required=False,
        choices=[("", "変更しない")] + NOTE_TEMPLATE_CHOICES,
        label="保存理由",
    )
    next_action = forms.CharField(
        required=False,
        label="次の行動",
        widget=forms.TextInput(attrs={"placeholder": "例: あとで読む / 比較する / 実装する"}),
    )
    recheck_at = forms.DateField(
        required=False,
        label="再確認日",
        widget=forms.DateInput(attrs={"type": "date"}),
    )
    clear_recheck_at = forms.BooleanField(required=False, label="再確認日をクリア")
    favorite_state = forms.ChoiceField(
        required=False,
        choices=[
            ("", "変更しない"),
            ("on", "お気に入りにする"),
            ("off", "お気に入りを外す"),
        ],
        label="お気に入り",
    )
    visibility_state = forms.ChoiceField(
        required=False,
        choices=[
            ("", "変更しない"),
            ("normal", "通常表示にする"),
            ("search_only", "検索時のみ表示にする"),
        ],
        label="一覧表示",
    )
    tags = forms.ModelMultipleChoiceField(
        queryset=Tag.objects.none(),
        required=False,
        label="追加タグ",
        widget=forms.SelectMultiple(attrs={"size": 5, "class": "compact-multiselect"}),
    )
    new_tags = forms.CharField(
        required=False,
        label="新しいタグを追加",
        widget=forms.Textarea(
            attrs={
                "rows": 2,
                "placeholder": "1行に1つずつ入力\n例:\n比較候補\n授業資料",
            }
        ),
        help_text="既存タグは追加、新規タグは作成して追加されます。",
    )

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.fields["tags"].queryset = Tag.objects.all().order_by("sort_order", "name")

    def clean_new_tags(self) -> list[str]:
        return parse_new_tag_names(self.cleaned_data.get("new_tags", ""))

    def clean(self):
        cleaned_data = super().clean()
        if cleaned_data.get("recheck_at") and cleaned_data.get("clear_recheck_at"):
            raise forms.ValidationError("再確認日を設定するときは、クリアを同時に指定できません。")
        has_updates = any(
            [
                cleaned_data.get("review_state"),
                cleaned_data.get("save_reason"),
                cleaned_data.get("next_action"),
                cleaned_data.get("recheck_at"),
                cleaned_data.get("clear_recheck_at"),
                cleaned_data.get("favorite_state"),
                cleaned_data.get("visibility_state"),
                cleaned_data.get("tags"),
                cleaned_data.get("new_tags"),
            ]
        )
        if not has_updates:
            raise forms.ValidationError("一括操作の内容を1つ以上指定してください。")
        return cleaned_data

    def resolve_tags(self) -> list[Tag]:
        resolved_tags = list(self.cleaned_data.get("tags") or [])
        existing_names = {tag.name.casefold(): tag for tag in resolved_tags}
        for name in self.cleaned_data.get("new_tags") or []:
            key = name.casefold()
            if key in existing_names:
                continue
            tag = Tag.objects.filter(name__iexact=name).first()
            if tag is None:
                tag = Tag.objects.create(name=name)
            existing_names[key] = tag
            resolved_tags.append(tag)
        return resolved_tags

    def apply_to_resources(self, resources) -> int:
        selected_resources = list(resources)
        tags_to_add = self.resolve_tags()
        review_state = self.cleaned_data.get("review_state") or ""
        save_reason = self.cleaned_data.get("save_reason") or ""
        next_action = (self.cleaned_data.get("next_action") or "").strip()
        recheck_at = self.cleaned_data.get("recheck_at")
        clear_recheck_at = self.cleaned_data.get("clear_recheck_at") or False
        favorite_state = self.cleaned_data.get("favorite_state") or ""
        visibility_state = self.cleaned_data.get("visibility_state") or ""

        with transaction.atomic():
            for resource in selected_resources:
                update_fields: list[str] = []
                if review_state:
                    resource.review_state = review_state
                    update_fields.append("review_state")
                if save_reason:
                    resource.save_reason = save_reason
                    update_fields.append("save_reason")
                if next_action:
                    resource.next_action = next_action
                    update_fields.append("next_action")
                if clear_recheck_at:
                    resource.recheck_at = None
                    update_fields.append("recheck_at")
                elif recheck_at:
                    resource.recheck_at = recheck_at
                    update_fields.append("recheck_at")
                if favorite_state == "on":
                    resource.favorite = True
                    update_fields.append("favorite")
                elif favorite_state == "off":
                    resource.favorite = False
                    update_fields.append("favorite")
                if visibility_state == "normal":
                    resource.search_only = False
                    update_fields.append("search_only")
                elif visibility_state == "search_only":
                    resource.search_only = True
                    update_fields.append("search_only")
                if update_fields:
                    resource.save(update_fields=[*dict.fromkeys(update_fields), "updated_at"])
                if tags_to_add:
                    resource.tags.add(*tags_to_add)
                    if not update_fields:
                        resource.save(update_fields=["updated_at"])
        return len(selected_resources)
