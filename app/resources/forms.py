from __future__ import annotations

from django import forms

from resources.models import Resource, ResourceStatus, ReviewState
from resources.services import normalize_url
from tags.models import Tag

NOTE_TEMPLATE_CHOICES = [
    ("後で読む", "後で読む"),
    ("消えそう", "消えそう"),
    ("参考実装", "参考実装"),
    ("就活用", "就活用"),
    ("買い物候補", "買い物候補"),
]


class ResourceForm(forms.ModelForm):
    note_template = forms.ChoiceField(
        required=False,
        choices=[("", "選択なし")] + NOTE_TEMPLATE_CHOICES,
        label="保存理由テンプレ",
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

    class Meta:
        model = Resource
        fields = ["original_url", "title_manual", "note", "favorite", "review_state"]
        labels = {
            "title_manual": "手動タイトル",
            "note": "メモ",
            "favorite": "お気に入り",
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
        self.fields["review_state"].label = "見直し状態"
        self.fields["review_state"].required = False
        self.fields["review_state"].initial = ReviewState.NONE
        self.fields["note_template"].help_text = "選ぶとメモの先頭に入ります。"
        self.order_fields(
            [
                "original_url",
                "title_manual",
                "note_template",
                "note",
                "review_state",
                "favorite",
                "tags",
            ]
        )
        if self.instance.pk:
            self.initial["tags"] = self.instance.tags.all()
            for template_value, _ in NOTE_TEMPLATE_CHOICES:
                note = (self.instance.note or "").strip()
                if note == template_value or note.startswith(f"{template_value}\n"):
                    self.initial["note_template"] = template_value
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

    def save(self, commit=True):
        resource = super().save(commit=False)
        resource.normalized_url = self.cleaned_normalized_url
        resource.update_domain_from_url()
        resource.review_state = self.cleaned_data.get("review_state") or ReviewState.NONE
        template = (self.cleaned_data.get("note_template") or "").strip()
        note = (resource.note or "").strip()
        if template:
            if not note:
                resource.note = template
            elif note != template and not note.startswith(f"{template}\n"):
                resource.note = f"{template}\n{note}"
            else:
                resource.note = note
        else:
            resource.note = note
        if commit:
            resource.save()
            resource.tags.set(self.cleaned_data["tags"])
        else:
            self._pending_tags = self.cleaned_data["tags"]
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
