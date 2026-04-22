from django.db import migrations, models


def copy_existing_ai_summary_to_translation(apps, schema_editor):
    Snapshot = apps.get_model("snapshots", "Snapshot")
    Snapshot.objects.exclude(ai_summary="").update(ai_translation=models.F("ai_summary"))


class Migration(migrations.Migration):

    dependencies = [
        ("snapshots", "0004_update_fetch_method_labels"),
    ]

    operations = [
        migrations.AddField(
            model_name="snapshot",
            name="ai_translation",
            field=models.TextField(blank=True),
        ),
        migrations.RunPython(
            copy_existing_ai_summary_to_translation,
            migrations.RunPython.noop,
        ),
    ]
