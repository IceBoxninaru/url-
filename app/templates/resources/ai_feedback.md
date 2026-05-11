{% autoescape off %}# {{ page_title }}

{{ page_description }}

- generated_at: {{ generated_at|date:"c" }}
- feedback: {{ feedback }}
- total_count: {{ total_count }}
- shown_count: {{ shown_count }}
- limit: {{ limit }}
- html_interested_url: {{ interested_url }}
- html_not_interested_url: {{ not_interested_url }}
- markdown_interested_url: {{ interested_markdown_url }}
- markdown_not_interested_url: {{ not_interested_markdown_url }}

{% for item in items %}
---

## {{ forloop.counter }}. {{ item.title }}

- resource_id: {{ item.id }}
- feedback: {{ item.feedback_label }} ({{ item.feedback }})
- interest_labels: {% if item.interest_label_names %}{{ item.interest_label_names|join:", " }}{% else %}-{% endif %}
- url: {{ item.url }}
- normalized_url: {{ item.normalized_url }}
- domain: {{ item.domain }}
- tags: {% if item.tags %}{{ item.tags|join:", " }}{% else %}-{% endif %}
- save_reason: {{ item.save_reason|default:"-" }}
- next_action: {{ item.next_action|default:"-" }}
- search_only: {{ item.search_only }}
- created_at: {{ item.created_at|date:"c" }}
- updated_at: {{ item.updated_at|date:"c" }}

{% if item.note %}### note

```text
{{ item.note }}
```

{% endif %}{% if item.snapshot %}### latest_snapshot

- snapshot_id: {{ item.snapshot.id }}
- snapshot_no: {{ item.snapshot.snapshot_no }}
- fetched_at: {{ item.snapshot.fetched_at|date:"c" }}
- page_title: {{ item.snapshot.page_title|default:"-" }}
- fetch_url: {{ item.snapshot.fetch_url }}
- fetch_method: {{ item.snapshot.fetch_method }}
- http_status: {{ item.snapshot.http_status|default:"-" }}
- category: {{ item.snapshot.category|default:"-" }}
- image_count: {{ item.snapshot.image_count }}
- video_count: {{ item.snapshot.video_count }}
- error_message: {{ item.snapshot.error_message|default:"-" }}

{% if item.snapshot.summary %}### summary

```text
{{ item.snapshot.summary }}
```

{% endif %}{% if item.snapshot.translation %}### translation

```text
{{ item.snapshot.translation }}
```

{% endif %}{% if item.snapshot.text_excerpt %}### text_excerpt

```text
{{ item.snapshot.text_excerpt }}
```

{% endif %}{% else %}latest_snapshot: none

{% endif %}{% empty %}{{ feedback_label }}のURLはまだありません。
{% endfor %}{% endautoescape %}
